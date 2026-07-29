Set-StrictMode -Version 2.0

function ConvertTo-Pr29ProcessId {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$InputObject,
        [ValidateSet('Auto', 'Integer', 'SystemDiagnosticsProcess', 'Win32Process')]
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
                [string]$cimClassProperty.Value.CimClassName -ne 'Win32_Process' -or
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

function Get-Pr29ListenerSnapshot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][ValidateRange(1, 65535)][int]$Port,
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
        $owningProcessProperty = $connection.PSObject.Properties['OwningProcess']
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

function Get-Pr29DescendantProcessIds {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][ValidateRange(1, 2147483647)]
        [int]$RootProcessId,
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [object[]]$ProcessSnapshot
    )

    $normalized = @()
    foreach ($processRecord in @($ProcessSnapshot)) {
        if ($null -eq $processRecord) {
            continue
        }
        $parentProperty = $processRecord.PSObject.Properties['ParentProcessId']
        if ($null -eq $parentProperty) {
            throw (
                'Win32_Process record does not expose ParentProcessId: ' +
                $processRecord.GetType().FullName
            )
        }
        $normalized += [pscustomobject]@{
            ProcessId = ConvertTo-Pr29ProcessId `
                -InputObject $processRecord `
                -ExpectedKind Win32Process
            ParentProcessId = [int]$parentProperty.Value
        }
    }

    $queue = New-Object System.Collections.Queue
    $queue.Enqueue($RootProcessId)
    $visited = @{}
    $descendants = @()
    while ($queue.Count -gt 0) {
        $parentProcessId = [int]$queue.Dequeue()
        foreach (
            $candidate in @(
                $normalized |
                    Where-Object { $_.ParentProcessId -eq $parentProcessId }
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

function Invoke-Pr29ProcessCleanup {
    [CmdletBinding()]
    param(
        $ServiceProcessId,
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
    $descendantsBeforeStop = @()
    $descendantsAfterParentStop = @()
    $forcedChildCleanupPids = @()
    $finalRemainingDescendants = @()
    $parentWasRunning = $false
    $parentStopRequested = $false
    $parentTerminated = $true
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
            "CLEANUP_START:$($_.Exception.GetType().Name):$($_.Exception.Message)"
        )
    }
    finally {
        if ($null -ne $ServiceProcessId) {
            $normalizedServiceProcessId = ConvertTo-Pr29ProcessId `
                -InputObject ([int64]$ServiceProcessId) `
                -ExpectedKind Integer
            try {
                $snapshot = @(& $ProcessSnapshotProvider)
                $descendantsBeforeStop = @(
                    Get-Pr29DescendantProcessIds `
                        -RootProcessId $normalizedServiceProcessId `
                        -ProcessSnapshot $snapshot
                )
            }
            catch {
                $cleanupErrors.Add(
                    "DESCENDANT_SNAPSHOT:$($_.Exception.GetType().Name):$($_.Exception.Message)"
                )
            }

            try {
                $parentWasRunning = $null -ne (
                    & $ProcessLookup $normalizedServiceProcessId
                )
                if ($parentWasRunning) {
                    $parentStopRequested = $true
                    & $StopAction $normalizedServiceProcessId
                }
            }
            catch {
                $cleanupErrors.Add(
                    "PARENT_STOP:$($_.Exception.GetType().Name):$($_.Exception.Message)"
                )
            }

            for (
                $waitAttempt = 0;
                $waitAttempt -lt $WaitIterations;
                $waitAttempt++
            ) {
                if (
                    $null -eq (
                        & $ProcessLookup $normalizedServiceProcessId
                    )
                ) {
                    break
                }
                & $DelayAction 100
            }

            $descendantsAfterParentStop = @(
                foreach ($candidateProcessId in $descendantsBeforeStop) {
                    if (
                        $null -ne (
                            & $ProcessLookup ([int]$candidateProcessId)
                        )
                    ) {
                        [int]$candidateProcessId
                    }
                }
            )
            foreach ($candidateProcessId in $descendantsAfterParentStop) {
                try {
                    $forcedChildCleanupPids += [int]$candidateProcessId
                    & $StopAction ([int]$candidateProcessId)
                }
                catch {
                    $cleanupErrors.Add(
                        "CHILD_STOP:$candidateProcessId`:" +
                        "$($_.Exception.GetType().Name):$($_.Exception.Message)"
                    )
                }
            }

            & $DelayAction 100
            $parentTerminated = $null -eq (
                & $ProcessLookup $normalizedServiceProcessId
            )
            $finalRemainingDescendants = @(
                foreach (
                    $candidateProcessId in @(
                        $descendantsBeforeStop +
                        $descendantsAfterParentStop
                    )
                ) {
                    if (
                        $null -ne (
                            & $ProcessLookup ([int]$candidateProcessId)
                        )
                    ) {
                        [int]$candidateProcessId
                    }
                }
            ) | Sort-Object -Unique
        }

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
                "PORT_RELEASE:$($_.Exception.GetType().Name):$($_.Exception.Message)"
            )
        }
    }

    $cleanupOk = (
        $parentTerminated -and
        @($finalRemainingDescendants).Count -eq 0 -and
        [int]$listenerSnapshot.Count -eq 0 -and
        $cleanupErrors.Count -eq 0
    )
    return [pscustomobject]@{
        service_pid = if ($null -ne $ServiceProcessId) {
            [int]$ServiceProcessId
        }
        else {
            $null
        }
        parent_was_running = $parentWasRunning
        parent_stop_requested = $parentStopRequested
        parent_terminated = $parentTerminated
        descendants_before_stop = @($descendantsBeforeStop)
        descendants_after_parent_stop = @($descendantsAfterParentStop)
        forced_child_cleanup_pids = @($forcedChildCleanupPids)
        final_remaining_descendants = @($finalRemainingDescendants)
        listener_count_after_stop = [int]$listenerSnapshot.Count
        listener_pids_after_stop = @($listenerSnapshot.ProcessIds)
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

    [System.Diagnostics.Process]$serviceProcessHandle = $null
    $serviceProcessId = $null
    $lifecycleError = $null
    $cleanupProof = $null
    try {
        $startedProcess = & $StartAction
        $serviceProcessHandle = $startedProcess
        $serviceProcessId = ConvertTo-Pr29ProcessId `
            -InputObject $serviceProcessHandle `
            -ExpectedKind SystemDiagnosticsProcess
        & $BodyAction ([int]$serviceProcessId) $serviceProcessHandle
    }
    catch {
        $lifecycleError = (
            "$($_.Exception.GetType().Name):$($_.Exception.Message)"
        )
    }
    finally {
        try {
            $cleanupProof = & $CleanupAction $serviceProcessId
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
        service_process_handle_type = if ($null -ne $serviceProcessHandle) {
            $serviceProcessHandle.GetType().FullName
        }
        else {
            $null
        }
        service_pid = if ($null -ne $serviceProcessId) {
            [int]$serviceProcessId
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
