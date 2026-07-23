function Get-SmokePollingDecision {
    param(
        [string]$RunStatus,
        [string]$JobStatus,
        [int]$QueueDepth = -1,
        [int]$RunningJobs = -1,
        [string]$AttemptStatus = ""
    )

    $successful = @("SUCCEEDED", "PARTIAL", "NO_DATA")
    $failed = @("FAILED", "TIMED_OUT", "CANCELLED", "REJECTED")
    $active = @("PENDING", "RUNNING", "RETRY_SCHEDULED")
    $attemptTerminal = @(
        "SUCCEEDED", "PARTIAL", "NO_DATA", "FAILED", "TIMED_OUT",
        "CANCELLED", "REJECTED", "ABANDONED"
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
