param(
    [string]$BaseUrl = "http://127.0.0.1:8000",
    [int]$TimeoutSeconds = 600,
    [string]$OutputDirectory = ".\data\market-research-smoke"
)

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\market_research_smoke_helpers.ps1"
$outputPath = Resolve-SmokeOutputDirectory -OutputDirectory $OutputDirectory
[System.IO.Directory]::CreateDirectory($outputPath) | Out-Null

$capabilities = $null
$queued = $null
$run = $null
$job = $null
$queue = $null
$latestAttempt = $null
$currentStep = $null
$decision = $null
$context = $null
$runId = $null
$jobId = $null
$failureMessage = $null
$startedAt = [DateTimeOffset]::UtcNow

try {
    $capabilities = Invoke-RestMethod -Method Get -Uri "$BaseUrl/ai-research/capabilities"
    $capabilities | ConvertTo-Json -Depth 10 |
        Set-Content -LiteralPath "$outputPath\capabilities.json" -Encoding utf8
    if ($capabilities.status -notin @("READY_TO_SMOKE", "LIVE_VERIFIED")) {
        throw "Research capability is not ready for an authorized smoke: $($capabilities.status)"
    }

    $request = @{
        force_requeue = $false
        correlation_id = "authorized-single-smoke"
        authorized_live_smoke = $true
    } | ConvertTo-Json
    $queued = Invoke-RestMethod -Method Post `
        -Uri "$BaseUrl/market-research/mnq/runs" `
        -ContentType "application/json" `
        -Body $request
    $queued | ConvertTo-Json -Depth 10 |
        Set-Content -LiteralPath "$outputPath\queued.json" -Encoding utf8
    $runId = [string]$queued.run_id
    $jobId = [string]$queued.job_id
    if ([string]::IsNullOrWhiteSpace($runId)) { throw "The service did not return run_id" }
    if ([string]::IsNullOrWhiteSpace($jobId)) { throw "The service did not return job_id" }

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $run = Invoke-RestMethod -Method Get -Uri "$BaseUrl/market-research/mnq/runs/$runId"
        $job = Invoke-RestMethod -Method Get -Uri "$BaseUrl/ai-research/jobs/$jobId"
        $queue = Invoke-RestMethod -Method Get -Uri "$BaseUrl/ai-research/status"
        $latestAttempt = @($job.attempt_history) | Select-Object -Last 1
        $currentStep = @($run.steps) |
            Where-Object { $_.status -in @("RUNNING", "FAILED") } |
            Select-Object -Last 1
        $decision = Get-SmokePollingDecision `
            -RunStatus ([string]$run.status) `
            -JobStatus ([string]$job.status) `
            -QueueDepth ([int]$queue.metrics.queue_depth) `
            -RunningJobs ([int]$queue.metrics.running_jobs) `
            -AttemptStatus ([string]$latestAttempt.status)
        if ($decision.done) { break }
        Start-Sleep -Seconds 5
    } while ([DateTimeOffset]::UtcNow -lt $deadline)

    if (-not $decision.done) {
        throw "Bounded polling expired for run $runId and job $jobId"
    }
    if ($decision.failed) {
        throw "Authorized smoke failed fast: $($decision.reason)"
    }

    $latest = Invoke-RestMethod -Method Get -Uri "$BaseUrl/market-research/mnq/latest"
    $context = Invoke-RestMethod -Method Get -Uri "$BaseUrl/market-context/mnq?refresh=false"
    $latest | ConvertTo-Json -Depth 20 |
        Set-Content -LiteralPath "$outputPath\latest.json" -Encoding utf8
    $context | ConvertTo-Json -Depth 20 |
        Set-Content -LiteralPath "$outputPath\market-context.json" -Encoding utf8

    $checksums = Get-ChildItem -LiteralPath $outputPath -Filter "*.json" | ForEach-Object {
        [ordered]@{
            file = $_.Name
            sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash
        }
    }
    $summary = [ordered]@{
        run_id = $runId
        job_id = $jobId
        terminal_status = $run.status
        snapshot_id = $context.snapshot_id
        snapshot_revision = $context.snapshot_revision
        artifacts = $checksums
        trading_or_order_endpoints_called = $false
        ai_trader_modified = $false
        requested_job_count = 1
    }
    $summary | ConvertTo-Json -Depth 10 |
        Set-Content -LiteralPath "$outputPath\summary.json" -Encoding utf8
    $summary
}
catch {
    $failureMessage = $_.Exception.Message
    try {
        $diagnostic = $latestAttempt.diagnostic
        if (-not $diagnostic) { $diagnostic = $job.last_diagnostic }
        $failureStep = $diagnostic.step
        if (-not $failureStep) { $failureStep = $currentStep.step_name }
        $failure = [ordered]@{
            run_id = $runId
            job_id = $jobId
            run_status = $run.status
            job_status = $job.status
            attempts = $job.attempts
            max_attempts = $job.max_attempts
            current_step = $currentStep.step_name
            step = $failureStep
            exit_code = $diagnostic.exit_code
            error_category = $diagnostic.category
            resource = $diagnostic.resource
            configured_limit = $diagnostic.configured_limit
            observed_count = $diagnostic.observed_count
            remaining_before_step = $diagnostic.remaining_before_step
            stderr_redacted = ConvertTo-SmokeSafeText $diagnostic.stderr_tail
            retry_classification = $diagnostic.retry_classification
            tool_events_observed = @(
                Get-SmokeCompactToolEvents $diagnostic.tool_events_observed
            )
            effective_usage = $diagnostic.effective_usage
            effective_budget = Get-SmokeCompactBudget $diagnostic.effective_budget
            diagnostic = [ordered]@{
                category = $diagnostic.category
                resource = $diagnostic.resource
                step = $diagnostic.step
                retry_classification = $diagnostic.retry_classification
                timestamp = $diagnostic.timestamp
            }
            capability_status = $capabilities.status
            queue_metrics = $queue.metrics
            polling_decision = $decision.reason
            error = ConvertTo-SmokeSafeText $failureMessage
            started_at = $startedAt.ToString("o")
            failed_at = [DateTimeOffset]::UtcNow.ToString("o")
        }
        Write-SmokeFailureReport -OutputPath $outputPath -Report $failure | Out-Null
    }
    catch {
        $reportWriteError = ConvertTo-SmokeSafeText $_.Exception.Message
        [Console]::Error.WriteLine(
            "Unable to write smoke failure report: $reportWriteError"
        )
    }
    throw
}
