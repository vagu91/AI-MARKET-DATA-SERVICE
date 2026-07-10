param(
    [string]$BaseUrl = "http://127.0.0.1:8011",
    [ValidateSet("false", "auto", "force")]
    [string]$RefreshMode = "false",
    [string]$OutputDirectory = ".\data\diagnostics\health_reports",
    [switch]$FailOnWarning,
    [switch]$OpenLatest,
    [switch]$Compact,
    [switch]$NoHistory
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$python = Join-Path $root ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $python)) {
    $python = "python"
}

$script = Join-Path $root "scripts\generate_health_report.py"
$argsList = @(
    $script,
    "--base-url", $BaseUrl,
    "--refresh", $RefreshMode,
    "--output-directory", $OutputDirectory
)
if ($FailOnWarning) { $argsList += "--fail-on-warning" }
if ($OpenLatest) { $argsList += "--open-latest" }
if ($Compact) { $argsList += "--compact" }
if ($NoHistory) { $argsList += "--no-history" }

& $python @argsList
exit $LASTEXITCODE
