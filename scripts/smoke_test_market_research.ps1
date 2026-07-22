param(
    [string]$BaseUrl = "http://127.0.0.1:8000",
    [int]$TimeoutSeconds = 600,
    [string]$OutputDirectory = ".\data\market-research-smoke"
)

$ErrorActionPreference = "Stop"
$outputPath = [System.IO.Path]::GetFullPath($OutputDirectory)
[System.IO.Directory]::CreateDirectory($outputPath) | Out-Null

$capabilities = Invoke-RestMethod -Method Get -Uri "$BaseUrl/ai-research/capabilities"
$capabilities | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath "$outputPath\capabilities.json" -Encoding utf8
if ($capabilities.status -notin @("READY_TO_SMOKE", "LIVE_VERIFIED")) {
    throw "Research capability is not ready for an authorized smoke: $($capabilities.status)"
}

$request = @{ force_requeue = $false; correlation_id = "authorized-single-smoke"; authorized_live_smoke = $true } | ConvertTo-Json
$queued = Invoke-RestMethod -Method Post -Uri "$BaseUrl/market-research/mnq/runs" -ContentType "application/json" -Body $request
$queued | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath "$outputPath\queued.json" -Encoding utf8
$runId = [string]$queued.run_id
if ([string]::IsNullOrWhiteSpace($runId)) { throw "The service did not return run_id" }

$deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
do {
    Start-Sleep -Seconds 5
    $status = Invoke-RestMethod -Method Get -Uri "$BaseUrl/market-research/mnq/runs/$runId"
    $status | ConvertTo-Json -Depth 30 | Set-Content -LiteralPath "$outputPath\status.json" -Encoding utf8
    if ($status.status -in @("SUCCEEDED", "PARTIAL", "NO_DATA", "FAILED")) { break }
} while ([DateTimeOffset]::UtcNow -lt $deadline)

if ($status.status -notin @("SUCCEEDED", "PARTIAL", "NO_DATA", "FAILED")) {
    throw "Bounded polling expired for run $runId"
}

$latest = Invoke-RestMethod -Method Get -Uri "$BaseUrl/market-research/mnq/latest"
$context = Invoke-RestMethod -Method Get -Uri "$BaseUrl/market-context/mnq?refresh=false"
$latest | ConvertTo-Json -Depth 30 | Set-Content -LiteralPath "$outputPath\latest.json" -Encoding utf8
$context | ConvertTo-Json -Depth 30 | Set-Content -LiteralPath "$outputPath\market-context.json" -Encoding utf8

$checksums = Get-ChildItem -LiteralPath $outputPath -Filter "*.json" | ForEach-Object {
    [ordered]@{ file = $_.Name; sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash }
}
$summary = [ordered]@{
    run_id = $runId
    terminal_status = $status.status
    snapshot_id = $context.snapshot_id
    snapshot_revision = $context.snapshot_revision
    artifacts = $checksums
    trading_or_order_endpoints_called = $false
    ai_trader_modified = $false
    requested_job_count = 1
}
$summary | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath "$outputPath\summary.json" -Encoding utf8
$summary
