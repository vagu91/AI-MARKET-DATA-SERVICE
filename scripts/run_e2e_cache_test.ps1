param(
    [string]$BaseUrl = "http://127.0.0.1:8000",
    [string]$Country = "US",
    [int]$Days = 30,
    [string]$Symbol = "MNQ"
)

$ErrorActionPreference = "Stop"
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outputDir = Join-Path "data/diagnostics" "e2e_run_$timestamp"
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

function Invoke-TestRun {
    param(
        [string]$Label,
        [string]$FileName,
        [bool]$ResetDb,
        [bool]$EnableAi,
        [string]$AiMode = "codex_cli"
    )

    $uri = "$BaseUrl/diagnostics/e2e-cache-test?country=$Country&days=$Days&symbol=$Symbol&reset_db=$($ResetDb.ToString().ToLower())&enable_ai=$($EnableAi.ToString().ToLower())&ai_mode=$AiMode&run_count=1"
    $response = Invoke-RestMethod -Method Post -Uri $uri
    $path = Join-Path $outputDir $FileName
    $response | ConvertTo-Json -Depth 80 | Set-Content -Encoding UTF8 -Path $path
    $run = $response.runs[0]
    Write-Host ""
    Write-Host $Label
    Write-Host "duration_ms=$($run.duration_ms) db_hits=$($run.db_hits) db_misses=$($run.db_misses) provider_hits=$($run.provider_hits) provider_failures=$($run.provider_failures)"
    Write-Host "ai_research_used=$($run.ai_research_used) ai_research_requests=$($run.ai_research_requests) facts_total=$($run.facts_total_after_run) news_total=$($run.news_total_after_run)"
    Write-Host "missing_critical_fields=$($run.missing_critical_fields.Count) output=$path"
}

Invoke-TestRun -Label "Test A - DB empty, AI disabled" -FileName "test_a_db_empty_ai_disabled.json" -ResetDb $true -EnableAi $false
Invoke-TestRun -Label "Test B - DB cache, AI disabled" -FileName "test_b_db_cache_ai_disabled.json" -ResetDb $false -EnableAi $false
Invoke-TestRun -Label "Test C - AI codex_cli fill" -FileName "test_c_ai_codex_fill.json" -ResetDb $false -EnableAi $true -AiMode "codex_cli"
Invoke-TestRun -Label "Test D - DB cache after AI" -FileName "test_d_db_cache_after_ai.json" -ResetDb $false -EnableAi $true -AiMode "codex_cli"

$summary = Invoke-RestMethod -Uri "$BaseUrl/diagnostics/db-summary"
$summaryPath = Join-Path $outputDir "db_summary.json"
$summary | ConvertTo-Json -Depth 80 | Set-Content -Encoding UTF8 -Path $summaryPath

$full = Invoke-RestMethod -Uri "$BaseUrl/diagnostics/full-model?symbol=$Symbol&country=$Country&days=$Days"
$fullPath = Join-Path $outputDir "final_market_context.json"
$full | ConvertTo-Json -Depth 100 | Set-Content -Encoding UTF8 -Path $fullPath

Write-Host ""
Write-Host "DB summary: $summaryPath"
Write-Host "Final market context: $fullPath"
Write-Host "All outputs saved in: $outputDir"
