$ErrorActionPreference = 'Stop'
Set-StrictMode -Version 2.0

$repoRoot = [System.IO.Path]::GetFullPath(
    (Join-Path $PSScriptRoot '..\..')
)
. (Join-Path $repoRoot 'scripts\pr29_process_lifecycle.ps1')

$pythonExe = (
    Resolve-Path (Join-Path $repoRoot '.venv\Scripts\python.exe')
).Path
$runtimeFile = (
    Resolve-Path (
        Join-Path $repoRoot 'tests\fixtures\pr29_local_validation_server.py'
    )
).Path
$sandboxPath = Join-Path $repoRoot 'data\unit-test-sandbox.sqlite'

function Assert-True {
    param(
        [Parameter(Mandatory = $true)][bool]$Condition,
        [Parameter(Mandatory = $true)][string]$Message
    )
    if (-not $Condition) {
        throw "ASSERT_TRUE_FAILED: $Message"
    }
}

function Assert-Equal {
    param(
        [Parameter(Mandatory = $true)]$Expected,
        [Parameter(Mandatory = $true)]$Actual,
        [Parameter(Mandatory = $true)][string]$Message
    )
    if ($Expected -ne $Actual) {
        throw (
            "ASSERT_EQUAL_FAILED: $Message; " +
            "expected=$Expected actual=$Actual"
        )
    }
}

function Assert-Throws {
    param(
        [Parameter(Mandatory = $true)][scriptblock]$Action,
        [Parameter(Mandatory = $true)][string]$Pattern,
        [Parameter(Mandatory = $true)][string]$Message
    )
    $caught = $null
    try {
        & $Action
    }
    catch {
        $caught = $_.Exception.Message
    }
    if ($null -eq $caught -or $caught -notmatch $Pattern) {
        throw (
            "ASSERT_THROWS_FAILED: $Message; " +
            "pattern=$Pattern caught=$caught"
        )
    }
}

function New-MockProcessRecord {
    param(
        [Parameter(Mandatory = $true)][int]$ProcessId,
        [Parameter(Mandatory = $true)][int]$ParentProcessId,
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string]$ExecutablePath,
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string]$CommandLine
    )
    return [pscustomobject]@{
        ProcessId = $ProcessId
        ParentProcessId = $ParentProcessId
        ExecutablePath = $ExecutablePath
        CommandLine = $CommandLine
    }
}

$tests = New-Object System.Collections.Generic.List[object]

$tests.Add([pscustomobject]@{
    Name = 'no listener'
    Action = {
        $snapshot = Get-Pr29ListenerSnapshot `
            -Port 8053 `
            -ConnectionProvider { param($RequestedPort) @() }
        Assert-Equal 0 $snapshot.Count 'zero listener count'
        Assert-Pr29ListenerExpectation `
            -Snapshot $snapshot `
            -ExpectedProcessId $null `
            -Phase 'test-no-listener'
    }
})

$tests.Add([pscustomobject]@{
    Name = 'one expected listener'
    Action = {
        $snapshot = Get-Pr29ListenerSnapshot `
            -Port 8053 `
            -ConnectionProvider {
                param($RequestedPort)
                @([pscustomobject]@{ OwningProcess = 4242 })
            }
        Assert-Equal 1 $snapshot.Count 'single listener count'
        Assert-Equal 4242 $snapshot.ProcessIds[0] 'single listener PID'
        Assert-Pr29ListenerExpectation `
            -Snapshot $snapshot `
            -ExpectedProcessId 4242 `
            -Phase 'test-expected-listener'
    }
})

$tests.Add([pscustomobject]@{
    Name = 'unexpected and multiple listeners'
    Action = {
        $unexpected = Get-Pr29ListenerSnapshot `
            -Port 8053 `
            -ConnectionProvider {
                param($RequestedPort)
                @([pscustomobject]@{ OwningProcess = 9999 })
            }
        Assert-Throws `
            -Action {
                Assert-Pr29ListenerExpectation `
                    -Snapshot $unexpected `
                    -ExpectedProcessId 4242 `
                    -Phase 'test-unexpected-listener'
            } `
            -Pattern 'Unexpected listener PID 9999' `
            -Message 'unexpected listener must fail'

        $multiple = Get-Pr29ListenerSnapshot `
            -Port 8053 `
            -ConnectionProvider {
                param($RequestedPort)
                @(
                    [pscustomobject]@{ OwningProcess = 4242 },
                    [pscustomobject]@{ OwningProcess = 4343 }
                )
            }
        Assert-Throws `
            -Action {
                Assert-Pr29ListenerExpectation `
                    -Snapshot $multiple `
                    -ExpectedProcessId 4242 `
                    -Phase 'test-multiple-listeners'
            } `
            -Pattern 'Expected one listener' `
            -Message 'multiple listeners must fail'
    }
})

$tests.Add([pscustomobject]@{
    Name = 'System.Diagnostics.Process identity'
    Action = {
        $handle = [System.Diagnostics.Process]::GetCurrentProcess()
        $normalized = ConvertTo-Pr29ProcessId `
            -InputObject $handle `
            -ExpectedKind SystemDiagnosticsProcess
        Assert-Equal $PID $normalized 'Start-Process handle uses Id'
    }
})

$tests.Add([pscustomobject]@{
    Name = 'Win32_Process identity'
    Action = {
        $win32Process = Get-CimInstance `
            Win32_Process `
            -Filter "ProcessId = $PID"
        $normalized = ConvertTo-Pr29ProcessId `
            -InputObject $win32Process `
            -ExpectedKind Win32Process
        Assert-Equal $PID $normalized 'Win32_Process uses ProcessId'
    }
})

$tests.Add([pscustomobject]@{
    Name = 'PID zero snapshot record is ignored'
    Action = {
        $snapshot = @(
            (New-MockProcessRecord `
                -ProcessId 0 `
                -ParentProcessId 0 `
                -ExecutablePath '' `
                -CommandLine '')
            (New-MockProcessRecord `
                -ProcessId 6102 `
                -ParentProcessId 6101 `
                -ExecutablePath $pythonExe `
                -CommandLine "`"$pythonExe`" -u `"$runtimeFile`"")
        )
        $descendants = @(
            Get-Pr29DescendantProcessIds `
                -RootProcessId 6101 `
                -ProcessSnapshot $snapshot
        )
        Assert-Equal 1 $descendants.Count 'only positive child retained'
        Assert-Equal 6102 $descendants[0] 'positive child identity'
    }
})

$tests.Add([pscustomobject]@{
    Name = 'same-process listener identity'
    Action = {
        $record = New-MockProcessRecord `
            -ProcessId 6201 `
            -ParentProcessId 100 `
            -ExecutablePath $pythonExe `
            -CommandLine "`"$pythonExe`" -u `"$runtimeFile`""
        $proof = Resolve-Pr29ServiceIdentity `
            -LauncherProcessId 6201 `
            -Port 18053 `
            -RuntimeFilePath $runtimeFile `
            -LauncherExecutablePath $pythonExe `
            -ExpectedSandboxDatabasePath $sandboxPath `
            -ActualSandboxDatabasePath $sandboxPath `
            -ProcessSnapshotProvider { @($record) } `
            -ConnectionProvider {
                param($RequestedPort)
                @([pscustomobject]@{ OwningProcess = 6201 })
            }
        Assert-True $proof.identity_verified 'same process identity'
        Assert-Equal 'SAME_PROCESS' $proof.process_relation 'same process relation'
    }
})

$tests.Add([pscustomobject]@{
    Name = 'direct-child listener identity'
    Action = {
        $launcher = New-MockProcessRecord `
            -ProcessId 6301 `
            -ParentProcessId 100 `
            -ExecutablePath $pythonExe `
            -CommandLine "`"$pythonExe`" -u `"$runtimeFile`""
        $listener = New-MockProcessRecord `
            -ProcessId 6302 `
            -ParentProcessId 6301 `
            -ExecutablePath $pythonExe `
            -CommandLine "`"$pythonExe`" -u `"$runtimeFile`""
        $proof = Resolve-Pr29ServiceIdentity `
            -LauncherProcessId 6301 `
            -Port 18053 `
            -RuntimeFilePath $runtimeFile `
            -LauncherExecutablePath $pythonExe `
            -ExpectedSandboxDatabasePath $sandboxPath `
            -ActualSandboxDatabasePath $sandboxPath `
            -ProcessSnapshotProvider { @($launcher, $listener) } `
            -ConnectionProvider {
                param($RequestedPort)
                @([pscustomobject]@{ OwningProcess = 6302 })
            }
        Assert-True $proof.identity_verified 'child identity'
        Assert-True $proof.parent_chain_verified 'child parent chain'
        Assert-Equal 'DIRECT_CHILD' $proof.process_relation 'child relation'
        Assert-Equal 6301 $proof.listener_parent_pid 'normalized parent PID'
    }
})

$tests.Add([pscustomobject]@{
    Name = 'unrelated listener is rejected'
    Action = {
        $launcher = New-MockProcessRecord `
            -ProcessId 6401 `
            -ParentProcessId 100 `
            -ExecutablePath $pythonExe `
            -CommandLine "`"$pythonExe`" -u `"$runtimeFile`""
        $listener = New-MockProcessRecord `
            -ProcessId 6402 `
            -ParentProcessId 9999 `
            -ExecutablePath $pythonExe `
            -CommandLine "`"$pythonExe`" -u `"$runtimeFile`""
        $proof = Resolve-Pr29ServiceIdentity `
            -LauncherProcessId 6401 `
            -Port 18053 `
            -RuntimeFilePath $runtimeFile `
            -LauncherExecutablePath $pythonExe `
            -ExpectedSandboxDatabasePath $sandboxPath `
            -ActualSandboxDatabasePath $sandboxPath `
            -ProcessSnapshotProvider { @($launcher, $listener) } `
            -ConnectionProvider {
                param($RequestedPort)
                @([pscustomobject]@{ OwningProcess = 6402 })
            }
        Assert-True (-not $proof.identity_verified) 'unrelated identity fails'
        Assert-Throws `
            -Action {
                Assert-Pr29ServiceIdentity `
                    -IdentityProof $proof `
                    -Phase 'unit-unrelated'
            } `
            -Pattern 'Service identity verification failed' `
            -Message 'unrelated listener assertion'
    }
})

$tests.Add([pscustomobject]@{
    Name = 'already terminated process skips descendant inspection'
    Action = {
        $snapshotCalls = New-Object System.Collections.Generic.List[string]
        $proof = Invoke-Pr29ProcessCleanup `
            -LauncherProcessId 6501 `
            -ListenerProcessId $null `
            -ProcessLookup { param($TargetProcessId) $null } `
            -StopAction { param($TargetProcessId) } `
            -ProcessSnapshotProvider {
                $snapshotCalls.Add('snapshot')
                @()
            } `
            -ConnectionProvider { param($RequestedPort) @() } `
            -DelayAction { param($Milliseconds) }
        Assert-True $proof.cleanup_ok 'already terminated cleanup'
        Assert-True $proof.launcher_terminated 'launcher termination proof'
        Assert-Equal 0 $snapshotCalls.Count 'no descendant inspection'
    }
})

$tests.Add([pscustomobject]@{
    Name = 'PID zero never reaches descendant inspection'
    Action = {
        $snapshotCalls = New-Object System.Collections.Generic.List[string]
        $proof = Invoke-Pr29ProcessCleanup `
            -LauncherProcessId 0 `
            -ListenerProcessId $null `
            -ProcessLookup { param($TargetProcessId) $null } `
            -StopAction { param($TargetProcessId) } `
            -ProcessSnapshotProvider {
                $snapshotCalls.Add('snapshot')
                @()
            } `
            -ConnectionProvider { param($RequestedPort) @() } `
            -DelayAction { param($Milliseconds) }
        Assert-True (-not $proof.cleanup_ok) 'PID zero is visible failure'
        Assert-Equal 0 $snapshotCalls.Count 'PID zero skips descendants'
        Assert-True (
            $proof.cleanup_errors[0] -match 'INVALID_PROCESS_ID:LAUNCHER:0'
        ) 'PID zero reason code'
    }
})

$tests.Add([pscustomobject]@{
    Name = 'launcher and listener cleanup are separate'
    Action = {
        $alive = @{
            6601 = $true
            6602 = $true
        }
        $stopCalls = New-Object System.Collections.Generic.List[int]
        $child = New-MockProcessRecord `
            -ProcessId 6602 `
            -ParentProcessId 6601 `
            -ExecutablePath $pythonExe `
            -CommandLine "`"$pythonExe`" -u `"$runtimeFile`""
        $system = New-MockProcessRecord `
            -ProcessId 0 `
            -ParentProcessId 0 `
            -ExecutablePath '' `
            -CommandLine ''
        $proof = Invoke-Pr29ProcessCleanup `
            -LauncherProcessId 6601 `
            -ListenerProcessId 6602 `
            -ProcessLookup {
                param($TargetProcessId)
                if ($alive[[int]$TargetProcessId]) {
                    [pscustomobject]@{ Id = [int]$TargetProcessId }
                }
            } `
            -StopAction {
                param($TargetProcessId)
                $stopCalls.Add([int]$TargetProcessId)
                $alive[[int]$TargetProcessId] = $false
            } `
            -ProcessSnapshotProvider { @($system, $child) } `
            -ConnectionProvider {
                param($RequestedPort)
                if ($alive[6602]) {
                    @([pscustomobject]@{ OwningProcess = 6602 })
                }
                else {
                    @()
                }
            } `
            -DelayAction { param($Milliseconds) }
        Assert-True $proof.cleanup_ok 'separate cleanup'
        Assert-True $proof.launcher_terminated 'launcher terminated'
        Assert-True $proof.listener_terminated 'listener terminated'
        Assert-True ($stopCalls -contains 6601) 'launcher stop'
        Assert-True ($stopCalls -contains 6602) 'listener stop'
        Assert-Equal 0 $proof.cleanup_errors.Count 'no PID zero error'
    }
})

$tests.Add([pscustomobject]@{
    Name = 'startup exception still invokes cleanup'
    Action = {
        $cleanupCalls = New-Object System.Collections.Generic.List[string]
        $result = Invoke-Pr29GuardedLifecycle `
            -StartAction { throw 'controlled startup exception' } `
            -BodyAction {
                param($LauncherProcessId, $LauncherProcessHandle)
                throw 'body must not run'
            } `
            -CleanupAction {
                param($LauncherProcessId, $ListenerProcessId)
                $cleanupCalls.Add('cleanup')
                [pscustomobject]@{
                    cleanup_ok = $true
                    cleanup_errors = @()
                }
            }
        Assert-Equal 1 $cleanupCalls.Count 'cleanup after startup failure'
        Assert-True (
            $result.lifecycle_error -match 'controlled startup exception'
        ) 'startup error retained'
        Assert-True (
            $null -eq $result.launcher_pid
        ) 'startup failure has no PID'
    }
})

$tests.Add([pscustomobject]@{
    Name = 'cleanup exception is contained'
    Action = {
        $result = Invoke-Pr29GuardedLifecycle `
            -StartAction {
                [System.Diagnostics.Process]::GetCurrentProcess()
            } `
            -BodyAction {
                param($LauncherProcessId, $LauncherProcessHandle)
                [pscustomobject]@{ listener_pid = $LauncherProcessId }
            } `
            -CleanupAction {
                param($LauncherProcessId, $ListenerProcessId)
                throw 'controlled cleanup exception'
            }
        Assert-True (-not $result.pass) 'cleanup exception fails lifecycle'
        Assert-True (
            $result.cleanup.cleanup_errors[0] -match
            'controlled cleanup exception'
        ) 'cleanup exception retained'
        Assert-Equal $PID $result.launcher_pid 'launcher PID normalized'
        Assert-Equal $PID $result.listener_pid 'listener PID normalized'
    }
})

$tests.Add([pscustomobject]@{
    Name = 'cleanup fault still terminates launcher and listener'
    Action = {
        $alive = @{
            6701 = $true
            6702 = $true
        }
        $child = New-MockProcessRecord `
            -ProcessId 6702 `
            -ParentProcessId 6701 `
            -ExecutablePath $pythonExe `
            -CommandLine "`"$pythonExe`" -u `"$runtimeFile`""
        $proof = Invoke-Pr29ProcessCleanup `
            -LauncherProcessId 6701 `
            -ListenerProcessId 6702 `
            -ProcessLookup {
                param($TargetProcessId)
                if ($alive[[int]$TargetProcessId]) {
                    [pscustomobject]@{ Id = [int]$TargetProcessId }
                }
            } `
            -StopAction {
                param($TargetProcessId)
                $alive[[int]$TargetProcessId] = $false
            } `
            -ProcessSnapshotProvider { @($child) } `
            -ConnectionProvider { param($RequestedPort) @() } `
            -DelayAction { param($Milliseconds) } `
            -FaultHook {
                param($Phase)
                if ($Phase -eq 'cleanup_start') {
                    throw 'controlled cleanup hook exception'
                }
            }
        Assert-True $proof.launcher_terminated 'launcher after fault'
        Assert-True $proof.listener_terminated 'listener after fault'
        Assert-Equal 0 $proof.final_remaining_descendants.Count 'no child'
        Assert-True (-not $proof.cleanup_ok) 'fault remains visible'
        Assert-True (
            $proof.cleanup_errors[0] -match
            'controlled cleanup hook exception'
        ) 'fault reason retained'
    }
})

$passed = 0
foreach ($test in $tests) {
    & $test.Action
    $passed++
    Write-Output ("PASS: " + $test.Name)
}
Write-Output "POWERSHELL_5_1_STRICTMODE_TESTS_PASS=$passed"
