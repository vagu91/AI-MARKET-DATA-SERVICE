param(
    [string]$OutputRoot = ''
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version 2.0

$repoRoot = [System.IO.Path]::GetFullPath(
    (Join-Path $PSScriptRoot '..\..')
)
$pythonCandidate = Join-Path $repoRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $pythonCandidate -PathType Leaf)) {
    throw "Required virtual-environment Python is missing: $pythonCandidate"
}
$pythonExe = (Resolve-Path -LiteralPath $pythonCandidate).Path
$runtimeFile = (
    Resolve-Path -LiteralPath (
        Join-Path $repoRoot 'tests\fixtures\pr29_local_validation_server.py'
    )
).Path
. (Join-Path $repoRoot 'scripts\pr29_process_lifecycle.ps1')

$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$temporaryOutput = [string]::IsNullOrWhiteSpace($OutputRoot)
if ($temporaryOutput) {
    $OutputRoot = Join-Path (
        [System.IO.Path]::GetTempPath()
    ) (
        'pr29-offline-harness-' + [guid]::NewGuid().ToString('N')
    )
}
$OutputRoot = [System.IO.Path]::GetFullPath($OutputRoot)
New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null

function Write-Utf8Json {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)]$Value
    )
    [System.IO.File]::WriteAllText(
        $Path,
        (($Value | ConvertTo-Json -Depth 100) + [Environment]::NewLine),
        $utf8NoBom
    )
}

function Assert-True {
    param(
        [Parameter(Mandatory = $true)][bool]$Condition,
        [Parameter(Mandatory = $true)][string]$Message
    )
    if (-not $Condition) {
        throw "ASSERT_TRUE_FAILED: $Message"
    }
}

function Test-ExactSetEqual {
    param(
        [AllowEmptyCollection()][object[]]$Left,
        [AllowEmptyCollection()][object[]]$Right
    )
    $leftSet = @($Left | ForEach-Object { [string]$_ } | Sort-Object -Unique)
    $rightSet = @(
        $Right | ForEach-Object { [string]$_ } | Sort-Object -Unique
    )
    return @(
        Compare-Object -ReferenceObject $leftSet -DifferenceObject $rightSet
    ).Count -eq 0
}

function Test-Disjoint {
    param(
        [AllowEmptyCollection()][object[]]$Left,
        [AllowEmptyCollection()][object[]]$Right
    )
    $leftSet = @{}
    foreach ($item in @($Left)) {
        $leftSet[[string]$item] = $true
    }
    foreach ($item in @($Right)) {
        if ($leftSet.ContainsKey([string]$item)) {
            return $false
        }
    }
    return $true
}

function Get-FreeLoopbackPort {
    $probe = New-Object System.Net.Sockets.TcpListener(
        [System.Net.IPAddress]::Loopback,
        0
    )
    try {
        $probe.Start()
        return [int]$probe.LocalEndpoint.Port
    }
    finally {
        $probe.Stop()
    }
}

$port = Get-FreeLoopbackPort
if ($port -eq 8053) {
    $port = Get-FreeLoopbackPort
}
if ($port -eq 8053) {
    throw 'Offline harness unexpectedly selected protected port 8053'
}

$sandboxDb = Join-Path $OutputRoot 'offline-sandbox.sqlite'
$stdoutPath = Join-Path $OutputRoot 'service-stdout.log'
$stderrPath = Join-Path $OutputRoot 'service-stderr.log'
$identityPath = Join-Path $OutputRoot 'process-identity.json'
$accountingPath = Join-Path $OutputRoot 'provider-accounting.json'
$cleanupPath = Join-Path $OutputRoot 'process-cleanup.json'
$negativeCasesPath = Join-Path $OutputRoot 'negative-cases.json'
$stagePath = Join-Path $OutputRoot 'offline-stage.json'
[System.IO.File]::WriteAllBytes($sandboxDb, [byte[]]@())

$environmentValues = [ordered]@{
    'PR29_OFFLINE_PORT' = [string]$port
    'PR29_OFFLINE_SANDBOX_DB' = $sandboxDb
    'NO_PROXY' = '127.0.0.1,localhost'
}
$previousEnvironment = @{}
foreach ($entry in $environmentValues.GetEnumerator()) {
    $previousEnvironment[$entry.Key] = (
        [Environment]::GetEnvironmentVariable($entry.Key, 'Process')
    )
    [Environment]::SetEnvironmentVariable(
        $entry.Key,
        [string]$entry.Value,
        'Process'
    )
}

[System.Diagnostics.Process]$launcherHandle = $null
$launcherPid = $null
$listenerPid = $null
$identityProof = $null
$cleanupProof = $null
$accountingProof = $null
$negativeCases = $null
$healthReached = $false
$mockRouteInvoked = $false
$stageError = $null

try {
    $launcherHandle = Start-Pr29ControlledPythonRuntime `
        -PythonExecutablePath $pythonExe `
        -RuntimeFilePath $runtimeFile `
        -WorkingDirectory $OutputRoot `
        -StandardOutputPath $stdoutPath `
        -StandardErrorPath $stderrPath
    $launcherPid = ConvertTo-Pr29ProcessId `
        -InputObject $launcherHandle `
        -ExpectedKind SystemDiagnosticsProcess

    for ($attempt = 0; $attempt -lt 120; $attempt++) {
        try {
            $health = Invoke-WebRequest `
                -UseBasicParsing `
                -Uri "http://127.0.0.1:$port/health" `
                -Method Get `
                -TimeoutSec 2
            if ($health.StatusCode -eq 200) {
                $healthReached = $true
                break
            }
        }
        catch {
        }
        Start-Sleep -Milliseconds 100
    }
    Assert-True $healthReached 'local health endpoint'

    $listenerSnapshot = Get-Pr29ListenerSnapshot -Port $port
    Assert-True (
        [int]$listenerSnapshot.Count -eq 1
    ) 'exactly one local listener'
    $listenerPid = ConvertTo-Pr29ProcessId `
        -InputObject ([int64]$listenerSnapshot.ProcessIds[0]) `
        -ExpectedKind Integer

    $controlsResponse = Invoke-WebRequest `
        -UseBasicParsing `
        -Uri "http://127.0.0.1:$port/controls" `
        -Method Get `
        -TimeoutSec 5
    $controls = $controlsResponse.Content | ConvertFrom-Json
    Assert-True (
        $controls.external_provider_network_enabled -eq $false
    ) 'provider network remains disabled'
    Assert-True ($controls.fixture_mode -eq $true) 'fixture controls'

    $identityProof = Resolve-Pr29ServiceIdentity `
        -LauncherProcessId $launcherPid `
        -Port $port `
        -RuntimeFilePath $runtimeFile `
        -LauncherExecutablePath $pythonExe `
        -ExpectedSandboxDatabasePath $sandboxDb `
        -ActualSandboxDatabasePath ([string]$controls.database_path)
    Assert-Pr29ServiceIdentity `
        -IdentityProof $identityProof `
        -Phase 'offline end-to-end'
    Write-Utf8Json -Path $identityPath -Value $identityProof

    $mockResponse = Invoke-WebRequest `
        -UseBasicParsing `
        -Uri "http://127.0.0.1:$port/mock/acquire" `
        -Method Post `
        -TimeoutSec 5
    Assert-True ($mockResponse.StatusCode -eq 200) 'mock acquisition status'
    $mockRouteInvoked = $true
    $fixture = $mockResponse.Content | ConvertFrom-Json

    $raw = @($fixture.raw_in_scope_ids)
    $parsed = @($fixture.parsed_ids)
    $technicalRejected = @($fixture.technically_rejected_ids)
    $persisted = @($fixture.persisted_ids)
    $persistenceRejected = @($fixture.persistence_rejected_ids)
    $outsideScope = @($fixture.explicit_outside_scope_ids)
    $delivered = @($fixture.delivered_ids)
    $quarantined = @($fixture.quarantined_ids)
    $withheld = @($fixture.withheld_ids)

    $rawRight = @($parsed + $technicalRejected)
    $parsedRight = @(
        $persisted + $persistenceRejected + $outsideScope
    )
    $persistedRight = @($delivered + $quarantined + $withheld)
    $accountingPass = (
        [int]$fixture.provider_network_calls -eq 0 -and
        (Test-ExactSetEqual -Left $raw -Right $rawRight) -and
        (Test-ExactSetEqual -Left $parsed -Right $parsedRight) -and
        (Test-ExactSetEqual -Left $persisted -Right $persistedRight) -and
        (Test-Disjoint -Left $parsed -Right $technicalRejected) -and
        (Test-Disjoint -Left $persisted -Right $persistenceRejected) -and
        (Test-Disjoint -Left $persisted -Right $outsideScope)
    )
    $accountingProof = [ordered]@{
        result = if ($accountingPass) { 'PASS' } else { 'FAIL' }
        provider_network_calls = [int]$fixture.provider_network_calls
        raw_in_scope_ids = $raw
        parsed_ids = $parsed
        technically_rejected_ids = $technicalRejected
        persisted_ids = $persisted
        persistence_rejected_ids = $persistenceRejected
        explicit_outside_scope_ids = $outsideScope
        delivered_ids = $delivered
        quarantined_ids = $quarantined
        withheld_ids = $withheld
        exact_identity_accounting = $accountingPass
    }
    Write-Utf8Json -Path $accountingPath -Value $accountingProof
    Assert-True $accountingPass 'fixture provider accounting'
}
catch {
    $stageError = (
        "$($_.Exception.GetType().Name):$($_.Exception.Message)"
    )
}
finally {
    try {
        $cleanupProof = Invoke-Pr29ProcessCleanup `
            -LauncherProcessId $launcherPid `
            -ListenerProcessId $listenerPid `
            -Port $port
    }
    catch {
        $cleanupProof = [pscustomobject]@{
            launcher_pid = $launcherPid
            listener_pid = $listenerPid
            cleanup_ok = $false
            cleanup_errors = @(
                "CLEANUP_UNHANDLED:" +
                "$($_.Exception.GetType().Name):$($_.Exception.Message)"
            )
        }
    }
    Write-Utf8Json -Path $cleanupPath -Value $cleanupProof
    foreach ($entry in $environmentValues.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable(
            $entry.Key,
            $previousEnvironment[$entry.Key],
            'Process'
        )
    }
}

$postflightListeners = Get-Pr29ListenerSnapshot -Port $port
$postflightPortFree = ([int]$postflightListeners.Count -eq 0)
$launcherResidual = (
    $null -ne $launcherPid -and
    $null -ne (Get-Process -Id $launcherPid -ErrorAction SilentlyContinue)
)
$listenerResidual = (
    $null -ne $listenerPid -and
    $null -ne (Get-Process -Id $listenerPid -ErrorAction SilentlyContinue)
)

$alreadyTerminatedSnapshotCalls = (
    New-Object System.Collections.Generic.List[string]
)
$alreadyTerminated = Invoke-Pr29ProcessCleanup `
    -LauncherProcessId 2147483000 `
    -ListenerProcessId $null `
    -Port $port `
    -ProcessLookup { param($TargetProcessId) $null } `
    -StopAction { param($TargetProcessId) } `
    -ProcessSnapshotProvider {
        $alreadyTerminatedSnapshotCalls.Add('called')
        @()
    } `
    -ConnectionProvider { param($RequestedPort) @() } `
    -DelayAction { param($Milliseconds) }

$zeroSnapshotCalls = New-Object System.Collections.Generic.List[string]
$zeroPid = Invoke-Pr29ProcessCleanup `
    -LauncherProcessId 0 `
    -ListenerProcessId $null `
    -Port $port `
    -ProcessLookup { param($TargetProcessId) $null } `
    -StopAction { param($TargetProcessId) } `
    -ProcessSnapshotProvider {
        $zeroSnapshotCalls.Add('called')
        @()
    } `
    -ConnectionProvider { param($RequestedPort) @() } `
    -DelayAction { param($Milliseconds) }

$unexpectedListenerRejected = $false
try {
    $unexpectedSnapshot = Get-Pr29ListenerSnapshot `
        -Port $port `
        -ConnectionProvider {
            param($RequestedPort)
            @([pscustomobject]@{ OwningProcess = 7777 })
        }
    Assert-Pr29ListenerExpectation `
        -Snapshot $unexpectedSnapshot `
        -ExpectedProcessId 8888 `
        -Phase 'offline negative listener'
}
catch {
    $unexpectedListenerRejected = (
        $_.Exception.Message -match 'Unexpected listener PID 7777'
    )
}

$negativeCases = [ordered]@{
    already_terminated_pass = (
        $alreadyTerminated.cleanup_ok -eq $true -and
        $alreadyTerminatedSnapshotCalls.Count -eq 0
    )
    pid_zero_rejected_without_descendant_snapshot = (
        $zeroPid.cleanup_ok -eq $false -and
        $zeroSnapshotCalls.Count -eq 0 -and
        @(
            $zeroPid.cleanup_errors |
                Where-Object {
                    $_ -match 'INVALID_PROCESS_ID:LAUNCHER:0'
                }
        ).Count -eq 1
    )
    unexpected_listener_rejected = $unexpectedListenerRejected
    child_listener_case_exercised = (
        $null -ne $identityProof -and
        $identityProof.process_relation -eq 'DIRECT_CHILD' -and
        $identityProof.parent_chain_verified -eq $true
    )
}
Write-Utf8Json -Path $negativeCasesPath -Value $negativeCases

$negativeCasesPass = (
    $negativeCases.already_terminated_pass -and
    $negativeCases.pid_zero_rejected_without_descendant_snapshot -and
    $negativeCases.unexpected_listener_rejected -and
    $negativeCases.child_listener_case_exercised
)
$stagePass = (
    $null -eq $stageError -and
    $healthReached -and
    $mockRouteInvoked -and
    $null -ne $identityProof -and
    $identityProof.identity_verified -eq $true -and
    $identityProof.parent_chain_verified -eq $true -and
    $null -ne $accountingProof -and
    $accountingProof.result -eq 'PASS' -and
    $null -ne $cleanupProof -and
    $cleanupProof.cleanup_ok -eq $true -and
    $postflightPortFree -and
    -not $launcherResidual -and
    -not $listenerResidual -and
    $negativeCasesPass
)

$stage = [ordered]@{
    phase = 'PR29_OFFLINE_VALIDATION_HARNESS_E2E'
    result = if ($stagePass) { 'PASS' } else { 'FAIL' }
    error = $stageError
    protected_live_port_8053_used = $false
    test_port = $port
    external_provider_network_calls = 0
    health_reached = $healthReached
    mock_route_invoked = $mockRouteInvoked
    process_identity = $identityProof
    provider_accounting = $accountingProof
    process_cleanup = $cleanupProof
    negative_cases = $negativeCases
    postflight_port_free = $postflightPortFree
    launcher_residual = $launcherResidual
    listener_residual = $listenerResidual
}
Write-Utf8Json -Path $stagePath -Value $stage

$secretPatterns = @(
    '-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----',
    '(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,})',
    '\bsk-[A-Za-z0-9]{20,}\b',
    '\bAKIA[0-9A-Z]{16}\b',
    '(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{24,}',
    (
        '(?i)(?:apikey|api_key|access_token|refresh_token|' +
        'client_secret|password)=[^&\s]{6,}'
    )
)
$secretFindings = @()
foreach ($artifactPath in @(
    $identityPath,
    $accountingPath,
    $cleanupPath,
    $negativeCasesPath,
    $stagePath,
    $stdoutPath,
    $stderrPath
)) {
    if (-not (Test-Path -LiteralPath $artifactPath -PathType Leaf)) {
        continue
    }
    foreach ($pattern in $secretPatterns) {
        $secretFindings += @(
            Select-String `
                -LiteralPath $artifactPath `
                -Pattern $pattern `
                -AllMatches
        )
    }
}
if ($secretFindings.Count -ne 0) {
    throw "Offline harness secret scan failed: $($secretFindings.Count)"
}

try {
    if (-not $stagePass) {
        throw (
            'OFFLINE_HARNESS_END_TO_END_FAIL: ' +
            ($stage | ConvertTo-Json -Depth 100 -Compress)
        )
    }
    [ordered]@{
        result = 'PASS'
        stage_artifact = $stagePath
        provider_accounting_artifact = $accountingPath
        process_identity_artifact = $identityPath
        process_cleanup_artifact = $cleanupPath
        negative_cases_artifact = $negativeCasesPath
        launcher_pid = $identityProof.launcher_pid
        listener_pid = $identityProof.listener_pid
        listener_parent_pid = $identityProof.listener_parent_pid
        process_relation = $identityProof.process_relation
        port = $port
        postflight_port_free = $postflightPortFree
        external_provider_network_calls = 0
        secret_findings = 0
    } | ConvertTo-Json -Depth 20
    Write-Output 'OFFLINE_HARNESS_END_TO_END_PASS'
}
finally {
    if ($temporaryOutput -and (Test-Path -LiteralPath $OutputRoot)) {
        Remove-Item -LiteralPath $OutputRoot -Recurse -Force
    }
}
