[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('Offline', 'ControlledLive')]
    [string]$Mode,
    [string]$OutputRoot = ''
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version 2.0

$repoRoot = (
    Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')
).Path
$pythonCandidate = Join-Path $repoRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $pythonCandidate -PathType Leaf)) {
    throw "Required virtual-environment Python is missing: $pythonCandidate"
}
$pythonExe = (Resolve-Path -LiteralPath $pythonCandidate).Path
$verifier = (
    Resolve-Path -LiteralPath (
        Join-Path $repoRoot 'scripts\verify_news_pipeline.py'
    )
).Path
$baseline = (
    Resolve-Path -LiteralPath (
        Join-Path $repoRoot 'docs\baselines\news-pipeline-v1.json'
    )
).Path

if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
    $stamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
    $OutputRoot = Join-Path (
        Join-Path $repoRoot 'data\news-pipeline-verification'
    ) $stamp
}
$OutputRoot = [System.IO.Path]::GetFullPath($OutputRoot)

& $pythonExe `
    $verifier `
    --mode $Mode `
    --repo-root $repoRoot `
    --output-root $OutputRoot `
    --baseline $baseline
if ($LASTEXITCODE -ne 0) {
    throw "News pipeline verification failed with exit code $LASTEXITCODE"
}
