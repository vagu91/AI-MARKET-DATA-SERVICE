$ErrorActionPreference = 'Stop'
Set-StrictMode -Version 2.0

$repoRoot = [System.IO.Path]::GetFullPath(
    (Join-Path $PSScriptRoot '..\..')
)
. (Join-Path $repoRoot 'scripts\pr29_process_lifecycle.ps1')

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
    Name = 'already terminated process'
    Action = {
        $stopCalls = New-Object System.Collections.Generic.List[int]
        $proof = Invoke-Pr29ProcessCleanup `
            -ServiceProcessId 5001 `
            -ProcessLookup { param($TargetProcessId) $null } `
            -StopAction {
                param($TargetProcessId)
                $stopCalls.Add([int]$TargetProcessId)
            } `
            -ProcessSnapshotProvider { @() } `
            -ConnectionProvider { param($RequestedPort) @() } `
            -DelayAction { param($Milliseconds) }
        Assert-True $proof.cleanup_ok 'already terminated cleanup'
        Assert-True $proof.parent_terminated 'parent termination proof'
        Assert-Equal 0 $stopCalls.Count 'already terminated is not stopped twice'
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
    Name = 'startup exception still invokes cleanup'
    Action = {
        $cleanupCalls = New-Object System.Collections.Generic.List[string]
        $result = Invoke-Pr29GuardedLifecycle `
            -StartAction { throw 'controlled startup exception' } `
            -BodyAction {
                param($ServiceProcessId, $ServiceProcessHandle)
                throw 'body must not run'
            } `
            -CleanupAction {
                param($ServiceProcessId)
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
            $null -eq $result.service_pid
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
                param($ServiceProcessId, $ServiceProcessHandle)
            } `
            -CleanupAction {
                param($ServiceProcessId)
                throw 'controlled cleanup exception'
            }
        Assert-True (-not $result.pass) 'cleanup exception fails lifecycle'
        Assert-True (
            $result.cleanup.cleanup_errors[0] -match
            'controlled cleanup exception'
        ) 'cleanup exception retained'
        Assert-Equal $PID $result.service_pid 'PID remains normalized integer'
    }
})

$tests.Add([pscustomobject]@{
    Name = 'cleanup fault still terminates parent and child'
    Action = {
        $alive = @{
            6101 = $true
            6102 = $true
        }
        $stopCalls = New-Object System.Collections.Generic.List[int]
        $currentWin32 = Get-CimInstance `
            Win32_Process `
            -Filter "ProcessId = $PID"
        $childRecord = $currentWin32 | Select-Object *
        $childRecord.ProcessId = 6102
        $childRecord.ParentProcessId = 6101
        $proof = Invoke-Pr29ProcessCleanup `
            -ServiceProcessId 6101 `
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
            -ProcessSnapshotProvider { @($childRecord) } `
            -ConnectionProvider { param($RequestedPort) @() } `
            -DelayAction { param($Milliseconds) } `
            -FaultHook {
                param($Phase)
                if ($Phase -eq 'cleanup_start') {
                    throw 'controlled cleanup hook exception'
                }
            }
        Assert-True $proof.parent_terminated 'parent terminated after fault'
        Assert-Equal 0 $proof.final_remaining_descendants.Count 'child terminated'
        Assert-True ($stopCalls -contains 6101) 'parent stop attempted'
        Assert-True ($stopCalls -contains 6102) 'child stop attempted'
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
