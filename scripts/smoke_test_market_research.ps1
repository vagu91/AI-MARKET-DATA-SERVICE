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
$parentRunId = $null
$queueContract = $null
$children = @()
$failedChildren = @()
$outcome = $null
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
    $queueContract = Resolve-SmokeQueueContract -Queued $queued -BaseUrl $BaseUrl
    $runId = [string]$queueContract.run_id
    $parentRunId = [string]$queueContract.parent_run_id
    $jobId = [string]$queueContract.job_id

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $run = Invoke-RestMethod -Method Get -Uri $queueContract.poll_url
        $queue = Invoke-RestMethod -Method Get -Uri "$BaseUrl/ai-research/status"
        if ($queueContract.is_parent) {
            $decision = Get-SmokeParentPollingDecision `
                -ParentStatus ([string]$run.status) `
                -ExpectedChildCount ([int]$run.expected_child_count) `
                -TerminalChildCount ([int]$run.terminal_child_count)
        }
        else {
            $job = Invoke-RestMethod -Method Get -Uri "$BaseUrl/ai-research/jobs/$jobId"
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
        }
        if ($decision.done) { break }
        Start-Sleep -Seconds 5
    } while ([DateTimeOffset]::UtcNow -lt $deadline)

    if (-not $decision.done) {
        throw "Bounded polling expired for run $runId and job $jobId"
    }
    if ($decision.failed) {
        throw "Authorized smoke failed fast: $($decision.reason)"
    }
    if ($queueContract.is_parent) {
        $children = @($run.children)
        if ($children.Count -ne [int]$run.expected_child_count) {
            throw "Parent returned $($children.Count) children; expected $($run.expected_child_count)"
        }
        $failedChildren = @(
            $children | Where-Object {
                $_.status -in @(
                    "FAILED", "LOOP_DETECTED", "TIMED_OUT", "CANCELLED", "REJECTED"
                )
            }
        )
        foreach ($child in $children) {
            $childJobId = [string]$child.child_job_id
            $childRunId = [string]$child.child_run_id
            if (-not [string]::IsNullOrWhiteSpace($childJobId)) {
                Invoke-RestMethod -Method Get -Uri "$BaseUrl/ai-research/jobs/$childJobId" |
                    ConvertTo-Json -Depth 20 |
                    Set-Content -LiteralPath "$outputPath\child-job-$childJobId.json" -Encoding utf8
            }
            if (-not [string]::IsNullOrWhiteSpace($childRunId)) {
                Invoke-RestMethod -Method Get `
                    -Uri "$BaseUrl/market-research/mnq/runs/$childRunId" |
                    ConvertTo-Json -Depth 20 |
                    Set-Content -LiteralPath "$outputPath\child-run-$childRunId.json" -Encoding utf8
            }
        }
    }
    $outcome = Get-SmokeOutcomeClassification `
        -ParentStatus ([string]$run.status) `
        -Children $children
    $failedChildren = @($outcome.failed_children)

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
        parent_run_id = if ($queueContract.is_parent) { $parentRunId } else { $null }
        job_id = if ($queueContract.is_parent) { $null } else { $jobId }
        manifest_id = $queueContract.manifest_id
        terminal_status = $run.status
        expected_child_count = if ($queueContract.is_parent) {
            [int]$run.expected_child_count
        } else { 1 }
        terminal_child_count = if ($queueContract.is_parent) {
            [int]$run.terminal_child_count
        } else { 1 }
        child_statuses = if ($queueContract.is_parent) {
            @($children | ForEach-Object {
                [ordered]@{
                    topic = $_.topic
                    job_id = $_.child_job_id
                    run_id = $_.child_run_id
                    status = $_.status
                }
            })
        } else { @() }
        failed_children = @($failedChildren)
        outcome_category = $outcome.category
        policy_no_data_children = @($outcome.policy_no_data_children)
        snapshot_id = $context.snapshot_id
        snapshot_revision = $context.snapshot_revision
        artifacts = $checksums
        trading_or_order_endpoints_called = $false
        ai_trader_modified = $false
        requested_job_count = if ($queueContract.is_parent) {
            [int]$run.expected_child_count
        } else { 1 }
        research_metrics = if ($queueContract.is_parent) {
            $run.telemetry
        } else {
            Get-SmokeCompactResearchMetrics $run.metrics
        }
        budget_mode = if ($queueContract.is_parent) {
            $run.telemetry.budget_mode
        } else {
            $run.metrics.budget_mode
        }
        threshold_exceeded = @(
            Get-SmokeThresholdExceeded $(
                if ($queueContract.is_parent) { $run.telemetry } else { $run.metrics }
            )
        )
        checkpoint = $run.checkpoint
        continuation_count = $run.continuation_count
    }
    $summary | ConvertTo-Json -Depth 10 |
        Set-Content -LiteralPath "$outputPath\summary.json" -Encoding utf8
    if ($outcome.failed) {
        throw "Authorized smoke contains internal child failures"
    }
    $summary
}
catch {
    $failureMessage = $_.Exception.Message
    try {
        $diagnostic = $currentStep.diagnostic
        if (-not $diagnostic) { $diagnostic = $latestAttempt.diagnostic }
        if (-not $diagnostic) { $diagnostic = $job.last_diagnostic }
        $failureStep = $diagnostic.step
        if (-not $failureStep) { $failureStep = $currentStep.step_name }
        $failure = [ordered]@{
            run_id = $runId
            parent_run_id = if ($queueContract -and $queueContract.is_parent) {
                $parentRunId
            } else { $null }
            job_id = if ($queueContract -and $queueContract.is_parent) {
                $null
            } else { $jobId }
            manifest_id = if ($queueContract) { $queueContract.manifest_id } else { $null }
            run_status = $run.status
            job_status = $job.status
            attempts = $job.attempts
            max_attempts = $job.max_attempts
            current_step = $currentStep.step_name
            step = $failureStep
            exit_code = $diagnostic.exit_code
            error_category = if ($diagnostic.category) {
                $diagnostic.category
            } elseif ($outcome) {
                $outcome.category
            } else {
                "internal_failure"
            }
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
            budget_mode = $run.metrics.budget_mode
            threshold_exceeded = @(
                Get-SmokeThresholdExceeded $(
                    if ($queueContract -and $queueContract.is_parent) {
                        $run.telemetry
                    } else {
                        $run.metrics
                    }
                )
            )
            research_metrics = Get-SmokeCompactResearchMetrics $run.metrics
            parent = if ($queueContract -and $queueContract.is_parent) {
                [ordered]@{
                    status = $run.status
                    expected_child_count = $run.expected_child_count
                    terminal_child_count = $run.terminal_child_count
                    checkpoint = $run.checkpoint
                    telemetry = $run.telemetry
                }
            } else { $null }
            failed_children = @($failedChildren)
            outcome_category = if ($outcome) { $outcome.category } else { "internal_failure" }
            policy_no_data_children = if ($outcome) {
                @($outcome.policy_no_data_children)
            } else {
                @()
            }
            progress = $diagnostic.progress
            loop_guard = [ordered]@{
                category = $diagnostic.category
                reason = $diagnostic.reason
                fingerprints = @($diagnostic.fingerprints) | Select-Object -Last 12
            }
            checkpoint = $run.checkpoint
            continuation_count = $run.continuation_count
            diagnostic = [ordered]@{
                category = $diagnostic.category
                exception_type = $diagnostic.exception_type
                message = ConvertTo-SmokeSafeText $diagnostic.message 500
                resource = $diagnostic.resource
                step = $diagnostic.step
                claim_ref = $diagnostic.claim_ref
                topic = $diagnostic.topic
                field_semantics = $diagnostic.field_semantics
                run_id = $diagnostic.run_id
                job_id = $diagnostic.job_id
                retry_classification = $diagnostic.retry_classification
                timestamp = $diagnostic.timestamp
                stack_fingerprint = $diagnostic.stack_fingerprint
                transaction_outcome = $diagnostic.transaction_outcome
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
