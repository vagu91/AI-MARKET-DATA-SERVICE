[CmdletBinding()]
param(
    [string[]]$Provider = @(),
    [string[]]$Dataset = @(),
    [string[]]$Metric = @(),
    [bool]$IncludeAI = $true,
    [ValidateRange(60, 14400)]
    [int]$TimeoutSeconds = 14400
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2.0
Add-Type -AssemblyName System.Net.Http -ErrorAction Stop

$ExpectedBranch = "codex/fix-senior-analyst-payload-quality"
$Repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Python = Join-Path $Repo ".venv\Scripts\python.exe"
$AuditScript = Join-Path $Repo "scripts\provider_capability_audit.py"
$RunId = [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssZ")
$OutputRoot = Join-Path $Repo "data\provider-capability-audit"
$RunnerLogRoot = Join-Path $Repo "data\provider-capability-audit-runner\$RunId"
$RunnerStdout = Join-Path $RunnerLogRoot "runner.stdout.log"
$RunnerStderr = Join-Path $RunnerLogRoot "runner.stderr.log"
$Process = $null
$ParentStartTicks = 0
$ObservedProcessStartTicks = @{}
$FullAudit = (
    $Provider.Count -eq 0 -and
    $Dataset.Count -eq 0 -and
    $Metric.Count -eq 0 -and
    $IncludeAI
)

function Quote-NativeArgument {
    param([Parameter(Mandatory = $true)][string]$Value)
    return '"' + $Value.Replace('"', '\"') + '"'
}

function Get-DescendantProcessIds {
    param([int]$ParentId)
    $found = New-Object "System.Collections.Generic.List[int]"
    $pending = New-Object "System.Collections.Generic.Queue[int]"
    $pending.Enqueue($ParentId)
    while ($pending.Count -gt 0) {
        $current = $pending.Dequeue()
        $children = @(
            Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
                Where-Object { [int]$_.ParentProcessId -eq $current }
        )
        foreach ($child in $children) {
            $childId = [int]$child.ProcessId
            if (-not $found.Contains($childId)) {
                $found.Add($childId)
                $pending.Enqueue($childId)
            }
        }
    }
    return @($found)
}

function Add-ObservedProcessIdentity {
    param([int]$ProcessId)
    if ($ProcessId -le 0 -or $ProcessId -eq $PID) {
        return
    }
    $observed = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
    if ($null -eq $observed) {
        return
    }
    try {
        $ObservedProcessStartTicks[$ProcessId] = [long]$observed.StartTime.ToUniversalTime().Ticks
    }
    catch {
        # Never retain a bare PID when its process identity cannot be verified.
    }
}

function Test-ObservedProcessIdentity {
    param(
        [int]$ProcessId,
        [long]$ExpectedStartTicks
    )
    $observed = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
    if ($null -eq $observed) {
        return $false
    }
    try {
        return [long]$observed.StartTime.ToUniversalTime().Ticks -eq $ExpectedStartTicks
    }
    catch {
        return $false
    }
}

function Stop-ControlledProcessTree {
    param(
        [int]$ParentId,
        [long]$ParentStartTicks,
        [hashtable]$ObservedIdentities = @{}
    )
    $parentOwned = Test-ObservedProcessIdentity `
        -ProcessId $ParentId `
        -ExpectedStartTicks $ParentStartTicks
    $descendants = if ($parentOwned) {
        @(Get-DescendantProcessIds -ParentId $ParentId)
    }
    else {
        @()
    }
    $verifiedObserved = @(
        foreach ($entry in $ObservedIdentities.GetEnumerator()) {
            if (
                Test-ObservedProcessIdentity `
                    -ProcessId ([int]$entry.Key) `
                    -ExpectedStartTicks ([long]$entry.Value)
            ) {
                [int]$entry.Key
            }
        }
    )
    $targets = @($descendants + $verifiedObserved) |
        Sort-Object -Unique -Descending
    foreach ($target in $targets) {
        if ($target -gt 0 -and $target -ne $PID) {
            Stop-Process -Id $target -Force -ErrorAction SilentlyContinue
        }
    }
    if ($parentOwned -and $ParentId -gt 0 -and $ParentId -ne $PID) {
        Stop-Process -Id $ParentId -Force -ErrorAction SilentlyContinue
    }
}

function Assert-GitScope {
    $branch = (git -C $Repo branch --show-current).Trim()
    if ($branch -ne $ExpectedBranch) {
        throw "Provider audit must run only on $ExpectedBranch."
    }
    $head = (git -C $Repo rev-parse HEAD).Trim()
    $remoteHead = (
        git -C $Repo rev-parse "origin/$ExpectedBranch"
    ).Trim()
    if ($head -ne $remoteHead) {
        throw "Provider audit requires the pushed HEAD of the existing PR branch."
    }
    $unexpected = @(
        @(git -C $Repo status --porcelain) |
            Where-Object { $_ -notmatch "^\?\? ai-trader-consumer-payload\.json$" }
    )
    if ($unexpected.Count -gt 0) {
        throw "Git worktree contains unexpected changes."
    }
}

Push-Location $Repo
try {
    if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) {
        throw "Project Python runtime not found at .venv\Scripts\python.exe."
    }
    if (-not (Test-Path -LiteralPath $AuditScript -PathType Leaf)) {
        throw "Provider Capability Audit entry point is missing."
    }
    Assert-GitScope
    New-Item -ItemType Directory -Path $RunnerLogRoot -Force | Out-Null

    $Arguments = New-Object "System.Collections.Generic.List[string]"
    $Arguments.Add($AuditScript)
    $Arguments.Add("--run-id")
    $Arguments.Add($RunId)
    $Arguments.Add("--output-root")
    $Arguments.Add($OutputRoot)
    foreach ($item in $Provider) {
        $Arguments.Add("--provider")
        $Arguments.Add($item)
    }
    foreach ($item in $Dataset) {
        $Arguments.Add("--dataset")
        $Arguments.Add($item)
    }
    foreach ($item in $Metric) {
        $Arguments.Add("--metric")
        $Arguments.Add($item)
    }
    if ($IncludeAI) {
        $Arguments.Add("--include-ai")
    }
    else {
        $Arguments.Add("--exclude-ai")
    }
    $ArgumentLine = (
        @($Arguments) | ForEach-Object { Quote-NativeArgument -Value $_ }
    ) -join " "

    $Process = Start-Process -FilePath $Python -WindowStyle Hidden -PassThru `
        -WorkingDirectory $Repo `
        -ArgumentList $ArgumentLine `
        -RedirectStandardOutput $RunnerStdout `
        -RedirectStandardError $RunnerStderr
    $ParentProcessId = [int]$Process.Id
    Add-ObservedProcessIdentity -ProcessId $ParentProcessId
    $ParentStartTicks = [long]$ObservedProcessStartTicks[$ParentProcessId]
    $Deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while (-not $Process.HasExited -and [DateTime]::UtcNow -lt $Deadline) {
        foreach ($processId in @(Get-DescendantProcessIds -ParentId $Process.Id)) {
            Add-ObservedProcessIdentity -ProcessId ([int]$processId)
        }
        Start-Sleep -Milliseconds 500
        $Process.Refresh()
    }
    if (-not $Process.HasExited) {
        throw "Provider Capability Audit exceeded its bounded runtime."
    }
    $Process.WaitForExit()
    if ($Process.ExitCode -ne 0) {
        $details = if (Test-Path -LiteralPath $RunnerStderr -PathType Leaf) {
            Get-Content -LiteralPath $RunnerStderr -Raw
        }
        else {
            "No stderr was captured."
        }
        throw "Provider Capability Audit failed: $details"
    }
}
finally {
    if ($null -ne $Process) {
        Stop-ControlledProcessTree `
            -ParentId ([int]$Process.Id) `
            -ParentStartTicks ([long]$ParentStartTicks) `
            -ObservedIdentities $ObservedProcessStartTicks
    }
    Pop-Location
}

$remaining = @(
    foreach ($entry in $ObservedProcessStartTicks.GetEnumerator()) {
        if (
            Test-ObservedProcessIdentity `
                -ProcessId ([int]$entry.Key) `
                -ExpectedStartTicks ([long]$entry.Value)
        ) {
            [int]$entry.Key
        }
    }
)
if ($remaining.Count -gt 0) {
    throw "Provider Capability Audit process cleanup failed."
}
$Latest = Join-Path $Repo "data\provider-capability-audit-latest.json"
$RunReport = Join-Path $OutputRoot "$RunId\audit-report.json"
if (-not (Test-Path -LiteralPath $RunReport -PathType Leaf)) {
    throw "Completed audit report is missing for run $RunId."
}
$Report = Get-Content -LiteralPath $RunReport -Raw | ConvertFrom-Json
if ($Report.run_id -ne $RunId -or $Report.audit_status -ne "COMPLETED") {
    throw "Run report does not identify a completed audit."
}
if ($FullAudit) {
    if (-not (Test-Path -LiteralPath $RunnerStdout -PathType Leaf)) {
        throw "Completed full audit did not emit its candidate pointer."
    }
    $SummaryLine = Get-Content -LiteralPath $RunnerStdout |
        Where-Object { $_.Trim().StartsWith("{") } |
        Select-Object -Last 1
    if (-not $SummaryLine) {
        throw "Completed full audit summary is missing."
    }
    try {
        $Summary = $SummaryLine | ConvertFrom-Json
    }
    catch {
        throw "Completed full audit summary is invalid JSON."
    }
    $Candidate = [string]$Summary.candidate_pointer
    if (
        $Summary.run_id -ne $RunId -or
        $Summary.audit_status -ne "COMPLETED" -or
        [string]::IsNullOrWhiteSpace($Candidate) -or
        -not (Test-Path -LiteralPath $Candidate -PathType Leaf)
    ) {
        throw "Completed full audit candidate pointer is invalid."
    }
    # Publication is deliberately the last state-changing step. The audit
    # subprocess has exited, every observed child is gone, the report is
    # complete, and the pushed branch is rechecked before strong acceptance.
    Assert-GitScope
    $PublicationOutput = @(
        & $Python $AuditScript "--publish-candidate" $Candidate 2>&1
    )
    if ($LASTEXITCODE -ne 0) {
        throw (
            "Completed full audit candidate failed strong publication: " +
            ($PublicationOutput -join [Environment]::NewLine)
        )
    }
    Write-Output "Latest pointer published: $Latest"
}
else {
    if (Test-Path -LiteralPath $Latest -PathType Leaf) {
        $Pointer = Get-Content -LiteralPath $Latest -Raw | ConvertFrom-Json
        if ($Pointer.run_id -eq $RunId) {
            throw "A filtered audit must not replace the full-audit latest pointer."
        }
    }
    Write-Output "Filtered audit completed; full-audit latest pointer was not changed."
}

Write-Output "Provider Capability Audit completed: $($Report.run_id)"
