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
