function Resolve-SmokeOutputDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$OutputDirectory
    )

    $resolved = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath(
        $OutputDirectory
    )
    return [System.IO.Path]::GetFullPath($resolved)
}

function ConvertTo-SmokeSafeText {
    param(
        [AllowNull()]
        [object]$Value,
        [int]$MaxLength = 1000
    )

    $text = [string]$Value
    $text = [regex]::Replace(
        $text,
        '(?i)(authorization\s*:\s*bearer|bearer|api[_-]?key|token|secret|password)(\s*[:=]?\s*)[^\s,;]+',
        '$1$2<redacted>'
    )
    if ($text.Length -gt $MaxLength) {
        return $text.Substring($text.Length - $MaxLength)
    }
    return $text
}

function ConvertTo-SmokeNonNullArray {
    param(
        [AllowNull()]
        [object]$Value
    )

    return @($Value) | Where-Object { $null -ne $_ }
}

function Resolve-SmokeQueueContract {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Queued,
        [Parameter(Mandatory = $true)]
        [string]$BaseUrl
    )

    $parentRunId = [string]$Queued.parent_run_id
    $runId = if (-not [string]::IsNullOrWhiteSpace($parentRunId)) {
        $parentRunId
    }
    else {
        [string]$Queued.run_id
    }
    if ([string]::IsNullOrWhiteSpace($runId)) {
        throw "The service did not return run_id or parent_run_id"
    }

    $jobId = [string]$Queued.job_id
    $isParent = -not [string]::IsNullOrWhiteSpace($parentRunId)
    if (-not $isParent -and [string]::IsNullOrWhiteSpace($jobId)) {
        throw "The legacy service contract did not return job_id"
    }
    $pollUrl = [string]$Queued.poll_url
    if ([string]::IsNullOrWhiteSpace($pollUrl)) {
        $pollUrl = "/market-research/mnq/runs/$runId"
    }
    if ($pollUrl -notmatch '^https?://') {
        $pollUrl = "$($BaseUrl.TrimEnd('/'))/$($pollUrl.TrimStart('/'))"
    }
    return [pscustomobject]@{
        is_parent = $isParent
        run_id = $runId
        parent_run_id = if ($isParent) { $parentRunId } else { $null }
        job_id = if ([string]::IsNullOrWhiteSpace($jobId)) { $null } else { $jobId }
        child_job_ids = @(
            ConvertTo-SmokeNonNullArray $Queued.child_job_ids |
                ForEach-Object { [string]$_ } |
                Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
        )
        manifest_id = $Queued.manifest_id
        poll_url = $pollUrl
    }
}

function Get-SmokeParentPollingDecision {
    param(
        [string]$ParentStatus,
        [int]$ExpectedChildCount = 0,
        [int]$TerminalChildCount = 0
    )

    $successful = @("SUCCEEDED", "PARTIAL", "NO_DATA")
    $failed = @("FAILED", "LOOP_DETECTED", "TIMED_OUT", "CANCELLED", "REJECTED")
    if ($ParentStatus -in $failed) {
        return [pscustomobject]@{
            done = $true
            failed = $true
            reason = "parent_terminal_failure:$ParentStatus"
        }
    }
    if ($ParentStatus -in $successful) {
        if ($TerminalChildCount -ne $ExpectedChildCount) {
            return [pscustomobject]@{
                done = $true
                failed = $true
                reason = "parent_terminal_child_count_mismatch:$TerminalChildCount/$ExpectedChildCount"
            }
        }
        return [pscustomobject]@{
            done = $true
            failed = $false
            reason = "parent_terminal_success:$ParentStatus"
        }
    }
    return [pscustomobject]@{
        done = $false
        failed = $false
        reason = "polling_parent"
    }
}

function Write-SmokeFailureReport {
    param(
        [Parameter(Mandatory = $true)]
        [string]$OutputPath,
        [Parameter(Mandatory = $true)]
        [System.Collections.IDictionary]$Report
    )

    [System.IO.Directory]::CreateDirectory($OutputPath) | Out-Null
    $reportPath = Join-Path -Path $OutputPath -ChildPath "failure-report.json"
    $absoluteReportPath = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath(
        $reportPath
    )
    $Report["report_path"] = [System.IO.Path]::GetFullPath($absoluteReportPath)
    $Report | ConvertTo-Json -Depth 6 -Compress |
        Set-Content -LiteralPath $Report["report_path"] -Encoding utf8
    return $Report["report_path"]
}

function Get-SmokeCompactToolEvents {
    param(
        [AllowNull()]
        [object[]]$Events
    )

    return @($Events) | Select-Object -Last 20 | ForEach-Object {
        [ordered]@{
            event_type = ConvertTo-SmokeSafeText $_.event_type 80
            raw_event_type = ConvertTo-SmokeSafeText $_.raw_event_type 120
            lifecycle = ConvertTo-SmokeSafeText $_.lifecycle 40
            item_id = ConvertTo-SmokeSafeText $_.item_id 200
            item_type = ConvertTo-SmokeSafeText $_.item_type 120
            phase = ConvertTo-SmokeSafeText $_.phase 80
            semantic_action = ConvertTo-SmokeSafeText $_.semantic_action 80
            query = ConvertTo-SmokeSafeText $_.query 300
            source_url = ConvertTo-SmokeSafeText $_.source_url 500
            canonical_url = ConvertTo-SmokeSafeText $_.canonical_url 500
            tool_action_fingerprint = ConvertTo-SmokeSafeText `
                $_.tool_action_fingerprint 64
            status = ConvertTo-SmokeSafeText $_.status 80
        }
    }
}

function Get-SmokeCompactBudget {
    param(
        [AllowNull()]
        [object]$Budget
    )

    if (-not $Budget) { return $null }
    return [ordered]@{
        budget_mode = $Budget.budget_mode
        max_searches = $Budget.max_searches
        max_opened_sources = $Budget.max_opened_sources
        remaining_searches = $Budget.remaining_searches
        remaining_opened_sources = $Budget.remaining_opened_sources
        daily_runs_remaining = $Budget.daily_runs_remaining
        daily_searches_remaining = $Budget.daily_searches_remaining
        daily_opened_sources_remaining = $Budget.daily_opened_sources_remaining
        remaining_runtime_seconds = $Budget.remaining_runtime_seconds
        threshold_exceeded = $Budget.threshold_exceeded
    }
}

function Get-SmokeCompactResearchMetrics {
    param(
        [AllowNull()]
        [object]$Metrics
    )

    if (-not $Metrics) { return $null }
    return [ordered]@{
        budget_mode = $Metrics.budget_mode
        raw_events_observed = $Metrics.raw_events_observed
        normalized_actions = $Metrics.normalized_actions
        deduplicated_tool_calls = $Metrics.deduplicated_tool_calls
        searches = $Metrics.searches
        opened_sources = $Metrics.opened_sources
        new_sources = $Metrics.new_sources
        claims_extracted = $Metrics.claims_extracted
        claims_accepted = $Metrics.claims_accepted
        claims_rejected = $Metrics.claims_rejected
        backend = $Metrics.backend
        duration_ms = $Metrics.duration_ms
        progress = $Metrics.progress
        usage = $Metrics.usage
        cost_status = $Metrics.cost_status
        threshold_warnings = @(
            ConvertTo-SmokeNonNullArray $Metrics.threshold_warnings |
                Select-Object -Last 20
        )
        loop_detections = $Metrics.loop_detections
        continuation_count = $Metrics.continuation_count
        checkpoint = $Metrics.checkpoint
        sources = $Metrics.sources
    }
}

function Get-SmokePollingDecision {
    param(
        [string]$RunStatus,
        [string]$JobStatus,
        [int]$QueueDepth = -1,
        [int]$RunningJobs = -1,
        [string]$AttemptStatus = ""
    )

    $successful = @("SUCCEEDED", "PARTIAL", "NO_DATA")
    $failed = @("FAILED", "LOOP_DETECTED", "TIMED_OUT", "CANCELLED", "REJECTED")
    $active = @("PENDING", "RUNNING", "RETRY_SCHEDULED")
    $attemptTerminal = @(
        "SUCCEEDED", "PARTIAL", "NO_DATA", "FAILED", "TIMED_OUT",
        "CANCELLED", "REJECTED", "LOOP_DETECTED", "CHECKPOINTED", "ABANDONED"
    )

    if ($JobStatus -in $failed) {
        return [pscustomobject]@{
            done = $true
            failed = $true
            reason = "job_terminal_failure:$JobStatus"
        }
    }
    if ($RunStatus -in $failed) {
        return [pscustomobject]@{
            done = $true
            failed = $true
            reason = "run_terminal_failure:$RunStatus"
        }
    }
    if (($JobStatus -in $successful) -and ($RunStatus -in $active)) {
        return [pscustomobject]@{
            done = $true
            failed = $true
            reason = "job_terminal_run_non_terminal:$JobStatus/$RunStatus"
        }
    }
    if (($RunStatus -in $successful) -and ($JobStatus -in $successful)) {
        return [pscustomobject]@{
            done = $true
            failed = $false
            reason = "terminal_success:$JobStatus/$RunStatus"
        }
    }
    if (
        ($QueueDepth -eq 0) -and
        ($RunningJobs -eq 0) -and
        ($AttemptStatus -in $attemptTerminal) -and
        (($RunStatus -in $active) -or ($JobStatus -in $active))
    ) {
        return [pscustomobject]@{
            done = $true
            failed = $true
            reason = "orphaned_non_terminal_state_with_empty_queue"
        }
    }
    return [pscustomobject]@{
        done = $false
        failed = $false
        reason = "polling"
    }
}
