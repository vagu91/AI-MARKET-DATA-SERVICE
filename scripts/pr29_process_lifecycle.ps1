Set-StrictMode -Version 2.0

function ConvertTo-Pr29ProcessId {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$InputObject,
        [ValidateSet(
            'Auto',
            'Integer',
            'SystemDiagnosticsProcess',
            'Win32Process'
        )]
        [string]$ExpectedKind = 'Auto'
    )

    $resolvedKind = $ExpectedKind
    if ($resolvedKind -eq 'Auto') {
        if (
            $InputObject -is [byte] -or
            $InputObject -is [int16] -or
            $InputObject -is [int32] -or
            $InputObject -is [int64]
        ) {
            $resolvedKind = 'Integer'
        }
        elseif ($InputObject -is [System.Diagnostics.Process]) {
            $resolvedKind = 'SystemDiagnosticsProcess'
        }
        else {
            $cimClassProperty = $InputObject.PSObject.Properties['CimClass']
            $processIdProperty = $InputObject.PSObject.Properties['ProcessId']
            if (
                $null -ne $cimClassProperty -and
                $null -ne $cimClassProperty.Value -and
                $null -ne $processIdProperty -and
                [string]$cimClassProperty.Value.CimClassName -eq 'Win32_Process'
            ) {
                $resolvedKind = 'Win32Process'
            }
            else {
                throw (
                    'Unsupported process identity object: ' +
                    $InputObject.GetType().FullName
                )
            }
        }
    }

    [int64]$rawProcessId = 0
    switch ($resolvedKind) {
        'Integer' {
            if (
                -not (
                    $InputObject -is [byte] -or
                    $InputObject -is [int16] -or
                    $InputObject -is [int32] -or
                    $InputObject -is [int64]
                )
            ) {
                throw 'Expected an integer process ID'
            }
            $rawProcessId = [int64]$InputObject
        }
        'SystemDiagnosticsProcess' {
            if (-not ($InputObject -is [System.Diagnostics.Process])) {
                throw (
                    'Expected System.Diagnostics.Process from ' +
                    'Start-Process -PassThru'
                )
            }
            $rawProcessId = [int64]$InputObject.Id
        }
        'Win32Process' {
            $cimClassProperty = $InputObject.PSObject.Properties['CimClass']
            $processIdProperty = $InputObject.PSObject.Properties['ProcessId']
            if (
                $null -eq $cimClassProperty -or
                $null -eq $cimClassProperty.Value -or
                [string]$cimClassProperty.Value.CimClassName -ne
                    'Win32_Process' -or
                $null -eq $processIdProperty
            ) {
                throw 'Expected a Win32_Process CIM instance with ProcessId'
            }
            $rawProcessId = [int64]$processIdProperty.Value
        }
    }

    if ($rawProcessId -le 0 -or $rawProcessId -gt [int]::MaxValue) {
        throw "Invalid process ID: $rawProcessId"
    }
    return [int]$rawProcessId
}

function Start-Pr29ControlledPythonRuntime {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$PythonExecutablePath,
        [Parameter(Mandatory = $true)][string]$RuntimeFilePath,
        [Parameter(Mandatory = $true)][string]$WorkingDirectory,
        [Parameter(Mandatory = $true)][string]$StandardOutputPath,
        [Parameter(Mandatory = $true)][string]$StandardErrorPath,
        [string[]]$AdditionalArguments = @()
    )

    $resolvedPython = (
        Resolve-Path -LiteralPath $PythonExecutablePath
    ).Path
    $resolvedRuntime = (
        Resolve-Path -LiteralPath $RuntimeFilePath
    ).Path
    $resolvedWorkingDirectory = (
        Resolve-Path -LiteralPath $WorkingDirectory
    ).Path

    $argumentList = New-Object System.Collections.Generic.List[string]
    [void]$argumentList.Add('-u')
    [void]$argumentList.Add(('"{0}"' -f $resolvedRuntime))
    foreach ($argument in @($AdditionalArguments)) {
        [void]$argumentList.Add([string]$argument)
    }

    [System.Diagnostics.Process]$processHandle = Start-Process `
        -FilePath $resolvedPython `
        -ArgumentList @($argumentList) `
        -WorkingDirectory $resolvedWorkingDirectory `
        -WindowStyle Hidden `
        -RedirectStandardOutput $StandardOutputPath `
        -RedirectStandardError $StandardErrorPath `
        -PassThru
    return $processHandle
}

function Get-Pr29ListenerSnapshot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateRange(1, 65535)]
        [int]$Port,
        [scriptblock]$ConnectionProvider
    )

    if ($null -eq $ConnectionProvider) {
        $ConnectionProvider = {
            param([int]$RequestedPort)
            return @(
                Get-NetTCPConnection `
                    -LocalPort $RequestedPort `
                    -State Listen `
                    -ErrorAction SilentlyContinue
            )
        }
    }

    $connections = @(& $ConnectionProvider $Port)
    $listenerPids = @()
    $entries = @()
    foreach ($connection in $connections) {
        if ($null -eq $connection) {
            continue
        }
        $owningProcessProperty = (
            $connection.PSObject.Properties['OwningProcess']
        )
        if ($null -eq $owningProcessProperty) {
            throw (
                'Listener object does not expose OwningProcess: ' +
                $connection.GetType().FullName
            )
        }
        $listenerProcessId = ConvertTo-Pr29ProcessId `
            -InputObject ([int64]$owningProcessProperty.Value) `
            -ExpectedKind Integer
        $listenerPids += $listenerProcessId
        $entries += [pscustomobject]@{
            LocalPort = $Port
            OwningProcessId = $listenerProcessId
            SourceType = $connection.GetType().FullName
        }
    }

    return [pscustomobject]@{
        Port = $Port
        Count = @($entries).Count
        ProcessIds = @($listenerPids | Sort-Object -Unique)
        Entries = @($entries)
    }
}

function Assert-Pr29ListenerExpectation {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Snapshot,
        $ExpectedProcessId,
        [Parameter(Mandatory = $true)][string]$Phase
    )

    $countProperty = $Snapshot.PSObject.Properties['Count']
    $processIdsProperty = $Snapshot.PSObject.Properties['ProcessIds']
    if ($null -eq $countProperty -or $null -eq $processIdsProperty) {
        throw "Invalid listener snapshot during $Phase"
    }

    $listenerCount = [int]$countProperty.Value
    $listenerPids = @($processIdsProperty.Value)
    if ($null -eq $ExpectedProcessId) {
        if ($listenerCount -eq 0) {
            return
        }
        throw (
            "Unexpected listener(s) during $Phase`: " +
            ($listenerPids -join ',')
        )
    }

    $expected = ConvertTo-Pr29ProcessId `
        -InputObject ([int64]$ExpectedProcessId) `
        -ExpectedKind Integer
    if ($listenerCount -eq 0) {
        throw "Expected listener PID $expected during $Phase, found zero"
    }
    if ($listenerCount -gt 1) {
        throw (
            "Expected one listener PID $expected during $Phase, found " +
            "$listenerCount`: $($listenerPids -join ',')"
        )
    }
    if ([int]$listenerPids[0] -ne $expected) {
        throw (
            "Unexpected listener PID $($listenerPids[0]) during $Phase; " +
            "expected $expected"
        )
    }
}

function ConvertTo-Pr29NormalizedProcessSnapshot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [object[]]$ProcessSnapshot
    )

    $normalized = @()
    foreach ($processRecord in @($ProcessSnapshot)) {
        if ($null -eq $processRecord) {
            continue
        }
        $processIdProperty = $processRecord.PSObject.Properties['ProcessId']
        $parentIdProperty = (
            $processRecord.PSObject.Properties['ParentProcessId']
        )
        if (
            $null -eq $processIdProperty -or
            $null -eq $parentIdProperty
        ) {
            throw (
                'Process record must expose ProcessId and ParentProcessId: ' +
                $processRecord.GetType().FullName
            )
        }

        [int64]$rawProcessId = 0
        [int64]$rawParentProcessId = 0
        $processIdValid = [int64]::TryParse(
            [string]$processIdProperty.Value,
            [ref]$rawProcessId
        )
        $parentIdValid = [int64]::TryParse(
            [string]$parentIdProperty.Value,
            [ref]$rawParentProcessId
        )
        if (
            -not $processIdValid -or
            $rawProcessId -le 0 -or
            $rawProcessId -gt [int]::MaxValue
        ) {
            continue
        }
        if (
            -not $parentIdValid -or
            $rawParentProcessId -lt 0 -or
            $rawParentProcessId -gt [int]::MaxValue
        ) {
            throw "Invalid ParentProcessId for PID $rawProcessId"
        }

        $executablePathProperty = (
            $processRecord.PSObject.Properties['ExecutablePath']
        )
        $commandLineProperty = (
            $processRecord.PSObject.Properties['CommandLine']
        )
        $normalized += [pscustomobject]@{
            ProcessId = [int]$rawProcessId
            ParentProcessId = if ($rawParentProcessId -gt 0) {
                [int]$rawParentProcessId
            }
            else {
                $null
            }
            ExecutablePath = if ($null -ne $executablePathProperty) {
                [string]$executablePathProperty.Value
            }
            else {
                ''
            }
            CommandLine = if ($null -ne $commandLineProperty) {
                [string]$commandLineProperty.Value
            }
            else {
                ''
            }
        }
    }
    return @($normalized)
}

function Get-Pr29DescendantProcessIds {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateRange(1, 2147483647)]
        [int]$RootProcessId,
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [object[]]$ProcessSnapshot
    )

    $normalized = @(
        ConvertTo-Pr29NormalizedProcessSnapshot `
            -ProcessSnapshot $ProcessSnapshot
    )
    $queue = New-Object System.Collections.Queue
    $queue.Enqueue($RootProcessId)
    $visited = @{}
    $descendants = @()
    while ($queue.Count -gt 0) {
        $parentProcessId = [int]$queue.Dequeue()
        foreach (
            $candidate in @(
                $normalized |
                    Where-Object {
                        $null -ne $_.ParentProcessId -and
                        [int]$_.ParentProcessId -eq $parentProcessId
                    }
            )
        ) {
            $candidateProcessId = [int]$candidate.ProcessId
            if (-not $visited.ContainsKey($candidateProcessId)) {
                $visited[$candidateProcessId] = $true
                $descendants += $candidateProcessId
                $queue.Enqueue($candidateProcessId)
            }
        }
    }
    return @($descendants | Sort-Object -Unique)
}

function ConvertTo-Pr29RedactedCommandLine {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$CommandLine,
        [Parameter(Mandatory = $true)][string]$RuntimeFilePath,
        [Parameter(Mandatory = $true)][string]$LauncherExecutablePath,
        [string]$ListenerExecutablePath = ''
    )

    $redacted = $CommandLine
    foreach ($replacement in @(
        [pscustomobject]@{
            Value = $RuntimeFilePath
            Token = '<CONTROLLED_RUNTIME_FILE>'
        },
        [pscustomobject]@{
            Value = $LauncherExecutablePath
            Token = '<LAUNCHER_PYTHON>'
        },
        [pscustomobject]@{
            Value = $ListenerExecutablePath
            Token = '<LISTENER_PYTHON>'
        }
    )) {
        if (-not [string]::IsNullOrWhiteSpace($replacement.Value)) {
            $redacted = [regex]::Replace(
                $redacted,
                [regex]::Escape($replacement.Value),
                $replacement.Token,
                [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
            )
        }
    }
    $redacted = [regex]::Replace(
        $redacted,
        '(?i)(api[_-]?key|access[_-]?token|refresh[_-]?token|' +
            'client[_-]?secret|password)(\s*=\s*|\s+)[^\s"]+',
        '$1=<REDACTED>'
    )
    return $redacted
}

function Resolve-Pr29ServiceIdentity {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateRange(1, 2147483647)]
        [int]$LauncherProcessId,
        [Parameter(Mandatory = $true)]
        [ValidateRange(1, 65535)]
        [int]$Port,
        [Parameter(Mandatory = $true)][string]$RuntimeFilePath,
        [Parameter(Mandatory = $true)][string]$LauncherExecutablePath,
        [Parameter(Mandatory = $true)][string]$ExpectedSandboxDatabasePath,
        [Parameter(Mandatory = $true)][string]$ActualSandboxDatabasePath,
        [scriptblock]$ProcessSnapshotProvider,
        [scriptblock]$ConnectionProvider
    )

    if ($null -eq $ProcessSnapshotProvider) {
        $ProcessSnapshotProvider = {
            return @(
                Get-CimInstance `
                    Win32_Process `
                    -ErrorAction SilentlyContinue
            )
        }
    }

    $launcherPid = ConvertTo-Pr29ProcessId `
        -InputObject ([int64]$LauncherProcessId) `
        -ExpectedKind Integer
    $listenerSnapshot = Get-Pr29ListenerSnapshot `
        -Port $Port `
        -ConnectionProvider $ConnectionProvider
    if ([int]$listenerSnapshot.Count -ne 1) {
        throw (
            "Expected exactly one listener on port $Port; found " +
            "$($listenerSnapshot.Count)"
        )
    }
    $listenerPid = ConvertTo-Pr29ProcessId `
        -InputObject ([int64]$listenerSnapshot.ProcessIds[0]) `
        -ExpectedKind Integer

    $processSnapshot = @(
        ConvertTo-Pr29NormalizedProcessSnapshot `
            -ProcessSnapshot @(& $ProcessSnapshotProvider)
    )
    $launcherRecords = @(
        $processSnapshot |
            Where-Object { [int]$_.ProcessId -eq $launcherPid }
    )
    $listenerRecords = @(
        $processSnapshot |
            Where-Object { [int]$_.ProcessId -eq $listenerPid }
    )
    if ($launcherRecords.Count -ne 1) {
        throw "Launcher PID $launcherPid is absent or ambiguous"
    }
    if ($listenerRecords.Count -ne 1) {
        throw "Listener PID $listenerPid is absent or ambiguous"
    }

    $launcher = $launcherRecords[0]
    $listener = $listenerRecords[0]
    $resolvedRuntime = [System.IO.Path]::GetFullPath($RuntimeFilePath)
    $resolvedLauncherExecutable = (
        Resolve-Path -LiteralPath $LauncherExecutablePath
    ).Path
    $resolvedExpectedSandbox = [System.IO.Path]::GetFullPath(
        $ExpectedSandboxDatabasePath
    )
    $resolvedActualSandbox = [System.IO.Path]::GetFullPath(
        $ActualSandboxDatabasePath
    )

    $launcherExecutableVerified = [string]::Equals(
        [System.IO.Path]::GetFullPath([string]$launcher.ExecutablePath),
        $resolvedLauncherExecutable,
        [System.StringComparison]::OrdinalIgnoreCase
    )
    $listenerExecutableName = [System.IO.Path]::GetFileName(
        [string]$listener.ExecutablePath
    )
    $listenerExecutableVerified = (
        -not [string]::IsNullOrWhiteSpace(
            [string]$listener.ExecutablePath
        ) -and
        $listenerExecutableName -in @('python.exe', 'pythonw.exe')
    )
    $launcherRuntimeVerified = (
        [string]$launcher.CommandLine
    ).IndexOf(
        $resolvedRuntime,
        [System.StringComparison]::OrdinalIgnoreCase
    ) -ge 0
    $listenerRuntimeVerified = (
        [string]$listener.CommandLine
    ).IndexOf(
        $resolvedRuntime,
        [System.StringComparison]::OrdinalIgnoreCase
    ) -ge 0

    $processRelation = 'UNRELATED'
    $parentChainVerified = $false
    if ($listenerPid -eq $launcherPid) {
        $processRelation = 'SAME_PROCESS'
        $parentChainVerified = $true
    }
    elseif (
        $null -ne $listener.ParentProcessId -and
        [int]$listener.ParentProcessId -eq $launcherPid
    ) {
        $processRelation = 'DIRECT_CHILD'
        $parentChainVerified = $true
    }

    $sandboxDatabaseVerified = [string]::Equals(
        $resolvedExpectedSandbox,
        $resolvedActualSandbox,
        [System.StringComparison]::OrdinalIgnoreCase
    )
    $identityVerified = (
        $launcherExecutableVerified -and
        $listenerExecutableVerified -and
        $launcherRuntimeVerified -and
        $listenerRuntimeVerified -and
        $parentChainVerified -and
        $sandboxDatabaseVerified
    )

    return [pscustomobject]@{
        launcher_pid = $launcherPid
        listener_pid = $listenerPid
        listener_parent_pid = if (
            $null -ne $listener.ParentProcessId
        ) {
            [int]$listener.ParentProcessId
        }
        else {
            $null
        }
        executable_path = [string]$listener.ExecutablePath
        launcher_executable_path = [string]$launcher.ExecutablePath
        command_line = ConvertTo-Pr29RedactedCommandLine `
            -CommandLine ([string]$listener.CommandLine) `
            -RuntimeFilePath $resolvedRuntime `
            -LauncherExecutablePath $resolvedLauncherExecutable `
            -ListenerExecutablePath ([string]$listener.ExecutablePath)
        runtime_file = $resolvedRuntime
        port = $Port
        process_relation = $processRelation
        launcher_executable_verified = $launcherExecutableVerified
        listener_executable_verified = $listenerExecutableVerified
        launcher_runtime_verified = $launcherRuntimeVerified
        listener_runtime_verified = $listenerRuntimeVerified
        sandbox_database_verified = $sandboxDatabaseVerified
        parent_chain_verified = $parentChainVerified
        identity_verified = $identityVerified
    }
}

function Assert-Pr29ServiceIdentity {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$IdentityProof,
        [Parameter(Mandatory = $true)][string]$Phase
    )

    if ($IdentityProof.identity_verified -ne $true) {
        throw (
            "Service identity verification failed during $Phase`: " +
            "launcher_pid=$($IdentityProof.launcher_pid); " +
            "listener_pid=$($IdentityProof.listener_pid); " +
            "listener_parent_pid=$($IdentityProof.listener_parent_pid); " +
            "relation=$($IdentityProof.process_relation); " +
            "launcher_executable=" +
            "$($IdentityProof.launcher_executable_verified); " +
            "listener_executable=" +
            "$($IdentityProof.listener_executable_verified); " +
            "launcher_runtime=" +
            "$($IdentityProof.launcher_runtime_verified); " +
            "listener_runtime=" +
            "$($IdentityProof.listener_runtime_verified); " +
            "sandbox=$($IdentityProof.sandbox_database_verified)"
        )
    }
}

function Invoke-Pr29ProcessCleanup {
    [CmdletBinding()]
    param(
        $LauncherProcessId,
        $ListenerProcessId,
        [ValidateRange(1, 65535)][int]$Port = 8053,
        [ValidateRange(1, 1000)][int]$WaitIterations = 150,
        [scriptblock]$ProcessLookup,
        [scriptblock]$StopAction,
        [scriptblock]$ProcessSnapshotProvider,
        [scriptblock]$ConnectionProvider,
        [scriptblock]$DelayAction,
        [scriptblock]$FaultHook
    )

    if ($null -eq $ProcessLookup) {
        $ProcessLookup = {
            param([int]$TargetProcessId)
            return Get-Process `
                -Id $TargetProcessId `
                -ErrorAction SilentlyContinue
        }
    }
    if ($null -eq $StopAction) {
        $StopAction = {
            param([int]$TargetProcessId)
            Stop-Process `
                -Id $TargetProcessId `
                -Force `
                -ErrorAction Stop
        }
    }
    if ($null -eq $ProcessSnapshotProvider) {
        $ProcessSnapshotProvider = {
            return @(
                Get-CimInstance `
                    Win32_Process `
                    -ErrorAction SilentlyContinue
            )
        }
    }
    if ($null -eq $DelayAction) {
        $DelayAction = {
            param([int]$Milliseconds)
            Start-Sleep -Milliseconds $Milliseconds
        }
    }

    $cleanupErrors = New-Object System.Collections.Generic.List[string]
    $invalidInputPids = New-Object System.Collections.Generic.List[string]
    $launcherPid = $null
    $listenerPid = $null
    foreach ($descriptor in @(
        [pscustomobject]@{
            Role = 'LAUNCHER'
            Value = $LauncherProcessId
        },
        [pscustomobject]@{
            Role = 'LISTENER'
            Value = $ListenerProcessId
        }
    )) {
        if ($null -eq $descriptor.Value) {
            continue
        }
        [int64]$candidate = 0
        $candidateValid = [int64]::TryParse(
            [string]$descriptor.Value,
            [ref]$candidate
        )
        if (
            -not $candidateValid -or
            $candidate -le 0 -or
            $candidate -gt [int]::MaxValue
        ) {
            $invalidInputPids.Add(
                "$($descriptor.Role):$($descriptor.Value)"
            )
            continue
        }
        if ($descriptor.Role -eq 'LAUNCHER') {
            $launcherPid = [int]$candidate
        }
        else {
            $listenerPid = [int]$candidate
        }
    }

    $launcherWasRunning = $false
    $listenerWasRunning = $false
    $launcherStopRequested = $false
    $listenerStopRequested = $false
    $launcherTerminated = $true
    $listenerTerminated = $true
    $descendantsBeforeStop = @()
    $stoppedDescendantPids = @()
    $finalRemainingDescendants = @()
    $listenerSnapshot = [pscustomobject]@{
        Port = $Port
        Count = 0
        ProcessIds = @()
        Entries = @()
    }

    try {
        if ($null -ne $FaultHook) {
            & $FaultHook 'cleanup_start'
        }
    }
    catch {
        $cleanupErrors.Add(
            "CLEANUP_START:$($_.Exception.GetType().Name):" +
            "$($_.Exception.Message)"
        )
    }
    finally {
        try {
            if ($null -ne $launcherPid) {
                $launcherWasRunning = $null -ne (
                    & $ProcessLookup $launcherPid
                )
            }
            if ($null -ne $listenerPid) {
                $listenerWasRunning = $null -ne (
                    & $ProcessLookup $listenerPid
                )
            }
        }
        catch {
            $cleanupErrors.Add(
                "PROCESS_LOOKUP:$($_.Exception.GetType().Name):" +
                "$($_.Exception.Message)"
            )
        }

        if ($launcherWasRunning) {
            try {
                $snapshot = @(& $ProcessSnapshotProvider)
                $descendantsBeforeStop = @(
                    Get-Pr29DescendantProcessIds `
                        -RootProcessId $launcherPid `
                        -ProcessSnapshot $snapshot
                )
            }
            catch {
                $cleanupErrors.Add(
                    "DESCENDANT_SNAPSHOT:$($_.Exception.GetType().Name):" +
                    "$($_.Exception.Message)"
                )
            }
        }

        if (
            $listenerWasRunning -and
            $null -ne $listenerPid -and
            $null -ne (& $ProcessLookup $listenerPid)
        ) {
            try {
                $listenerStopRequested = $true
                & $StopAction $listenerPid
            }
            catch {
                if (
                    $null -ne (
                        & $ProcessLookup $listenerPid
                    )
                ) {
                    $cleanupErrors.Add(
                        "LISTENER_STOP:$listenerPid`:" +
                        "$($_.Exception.GetType().Name):" +
                        "$($_.Exception.Message)"
                    )
                }
            }
        }

        foreach (
            $candidatePid in @(
                $descendantsBeforeStop |
                    Where-Object {
                        $null -eq $listenerPid -or
                        [int]$_ -ne $listenerPid
                    }
            )
        ) {
            if (
                $null -ne (
                    & $ProcessLookup ([int]$candidatePid)
                )
            ) {
                try {
                    $stoppedDescendantPids += [int]$candidatePid
                    & $StopAction ([int]$candidatePid)
                }
                catch {
                    if (
                        $null -ne (
                            & $ProcessLookup ([int]$candidatePid)
                        )
                    ) {
                        $cleanupErrors.Add(
                            "DESCENDANT_STOP:$candidatePid`:" +
                            "$($_.Exception.GetType().Name):" +
                            "$($_.Exception.Message)"
                        )
                    }
                }
            }
        }

        if (
            $launcherWasRunning -and
            $null -ne $launcherPid -and
            $null -ne (& $ProcessLookup $launcherPid) -and
            (
                $null -eq $listenerPid -or
                $launcherPid -ne $listenerPid
            )
        ) {
            try {
                $launcherStopRequested = $true
                & $StopAction $launcherPid
            }
            catch {
                if (
                    $null -ne (
                        & $ProcessLookup $launcherPid
                    )
                ) {
                    $cleanupErrors.Add(
                        "LAUNCHER_STOP:$launcherPid`:" +
                        "$($_.Exception.GetType().Name):" +
                        "$($_.Exception.Message)"
                    )
                }
            }
        }
        elseif (
            $launcherWasRunning -and
            $null -ne $launcherPid -and
            $launcherPid -eq $listenerPid
        ) {
            $launcherStopRequested = $listenerStopRequested
        }

        for (
            $waitAttempt = 0;
            $waitAttempt -lt $WaitIterations;
            $waitAttempt++
        ) {
            $launcherAlive = (
                $null -ne $launcherPid -and
                $null -ne (& $ProcessLookup $launcherPid)
            )
            $listenerAlive = (
                $null -ne $listenerPid -and
                $null -ne (& $ProcessLookup $listenerPid)
            )
            $descendantAlive = @(
                foreach ($candidatePid in $descendantsBeforeStop) {
                    if (
                        $null -ne (
                            & $ProcessLookup ([int]$candidatePid)
                        )
                    ) {
                        [int]$candidatePid
                    }
                }
            )
            if (
                -not $launcherAlive -and
                -not $listenerAlive -and
                $descendantAlive.Count -eq 0
            ) {
                break
            }
            & $DelayAction 100
        }

        if ($null -ne $launcherPid) {
            $launcherTerminated = $null -eq (
                & $ProcessLookup $launcherPid
            )
        }
        if ($null -ne $listenerPid) {
            $listenerTerminated = $null -eq (
                & $ProcessLookup $listenerPid
            )
        }
        $finalRemainingDescendants = @(
            foreach ($candidatePid in $descendantsBeforeStop) {
                if (
                    $null -ne (
                        & $ProcessLookup ([int]$candidatePid)
                    )
                ) {
                    [int]$candidatePid
                }
            }
        ) | Sort-Object -Unique

        try {
            $listenerSnapshot = Get-Pr29ListenerSnapshot `
                -Port $Port `
                -ConnectionProvider $ConnectionProvider
            Assert-Pr29ListenerExpectation `
                -Snapshot $listenerSnapshot `
                -ExpectedProcessId $null `
                -Phase 'cleanup'
        }
        catch {
            $cleanupErrors.Add(
                "PORT_RELEASE:$($_.Exception.GetType().Name):" +
                "$($_.Exception.Message)"
            )
        }
    }

    foreach ($invalidPid in $invalidInputPids) {
        $cleanupErrors.Add("INVALID_PROCESS_ID:$invalidPid")
    }
    $cleanupOk = (
        $launcherTerminated -and
        $listenerTerminated -and
        @($finalRemainingDescendants).Count -eq 0 -and
        [int]$listenerSnapshot.Count -eq 0 -and
        $cleanupErrors.Count -eq 0
    )
    return [pscustomobject]@{
        launcher_pid = $launcherPid
        listener_pid = $listenerPid
        launcher_was_running = $launcherWasRunning
        listener_was_running = $listenerWasRunning
        launcher_stop_requested = $launcherStopRequested
        listener_stop_requested = $listenerStopRequested
        launcher_terminated = $launcherTerminated
        listener_terminated = $listenerTerminated
        descendants_before_stop = @($descendantsBeforeStop)
        stopped_descendant_pids = @($stoppedDescendantPids)
        final_remaining_descendants = @($finalRemainingDescendants)
        listener_count_after_stop = [int]$listenerSnapshot.Count
        listener_pids_after_stop = @($listenerSnapshot.ProcessIds)
        invalid_input_pids = @($invalidInputPids)
        cleanup_errors = @($cleanupErrors)
        cleanup_ok = $cleanupOk
    }
}

function Invoke-Pr29GuardedLifecycle {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][scriptblock]$StartAction,
        [Parameter(Mandatory = $true)][scriptblock]$BodyAction,
        [Parameter(Mandatory = $true)][scriptblock]$CleanupAction
    )

    [System.Diagnostics.Process]$launcherProcessHandle = $null
    $launcherProcessId = $null
    $listenerProcessId = $null
    $lifecycleError = $null
    $cleanupProof = $null
    try {
        $launcherProcessHandle = & $StartAction
        $launcherProcessId = ConvertTo-Pr29ProcessId `
            -InputObject $launcherProcessHandle `
            -ExpectedKind SystemDiagnosticsProcess
        $bodyResult = & $BodyAction `
            ([int]$launcherProcessId) `
            $launcherProcessHandle
        if (
            $null -ne $bodyResult -and
            $null -ne $bodyResult.PSObject.Properties['listener_pid']
        ) {
            $listenerProcessId = ConvertTo-Pr29ProcessId `
                -InputObject ([int64]$bodyResult.listener_pid) `
                -ExpectedKind Integer
        }
    }
    catch {
        $lifecycleError = (
            "$($_.Exception.GetType().Name):$($_.Exception.Message)"
        )
    }
    finally {
        try {
            $cleanupProof = & $CleanupAction `
                $launcherProcessId `
                $listenerProcessId
        }
        catch {
            $cleanupProof = [pscustomobject]@{
                cleanup_ok = $false
                cleanup_errors = @(
                    "CLEANUP_UNHANDLED:" +
                    "$($_.Exception.GetType().Name):$($_.Exception.Message)"
                )
            }
        }
    }

    return [pscustomobject]@{
        launcher_process_handle_type = if (
            $null -ne $launcherProcessHandle
        ) {
            $launcherProcessHandle.GetType().FullName
        }
        else {
            $null
        }
        launcher_pid = if ($null -ne $launcherProcessId) {
            [int]$launcherProcessId
        }
        else {
            $null
        }
        listener_pid = if ($null -ne $listenerProcessId) {
            [int]$listenerProcessId
        }
        else {
            $null
        }
        lifecycle_error = $lifecycleError
        cleanup = $cleanupProof
        pass = (
            $null -eq $lifecycleError -and
            $null -ne $cleanupProof -and
            $cleanupProof.cleanup_ok -eq $true
        )
    }
}
