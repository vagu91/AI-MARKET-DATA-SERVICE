[CmdletBinding()]
param(
    [ValidateRange(1, 65535)]
    [int]$Port = 8053,
    [ValidateRange(60, 1800)]
    [int]$RequestTimeoutSeconds = 1200
)

$ErrorActionPreference = "Stop"
$ExpectedBranch = "codex/fix-senior-analyst-payload-quality"
$ExpectedBase = "3d91f457ae67130a6481bdad59403068ab802bab"
$Repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Python = Join-Path $Repo ".venv\Scripts\python.exe"
$RunId = [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssZ")
$Output = Join-Path $Repo "data\senior-analyst-live-validation\$RunId"
$BodyPath = Join-Path $Output "response-body.json"
$HeadersPath = Join-Path $Output "response-headers.json"
$ReportPath = Join-Path $Output "acceptance-report.json"
$LatestPath = Join-Path $Repo "data\senior-analyst-live-latest.json"
$ServiceOut = Join-Path $Output "service.stdout.log"
$ServiceErr = Join-Path $Output "service.stderr.log"
$Process = $null
$CleanupOk = $false
$RequestStartedAt = $null

function Assert-PortFree {
    param([int]$LocalPort)
    $listener = Get-NetTCPConnection -LocalPort $LocalPort -State Listen -ErrorAction SilentlyContinue
    if ($listener) {
        throw "Port $LocalPort is already listening."
    }
}

function Stop-ProcessTree {
    param([int]$ParentId)
    $children = @(
        Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object { $_.ParentProcessId -eq $ParentId }
    )
    foreach ($child in $children) {
        Stop-ProcessTree -ParentId ([int]$child.ProcessId)
    }
    Stop-Process -Id $ParentId -Force -ErrorAction SilentlyContinue
}

function Save-DatabaseBundle {
    param([string]$DatabasePath, [string]$Destination)
    $database = [IO.Path]::GetFullPath($DatabasePath)
    if (-not (Test-Path -LiteralPath $database -PathType Leaf)) {
        throw "Operational database not found."
    }
    New-Item -ItemType Directory -Path $Destination -Force | Out-Null
    $sourceFiles = @($database, "$database-wal", "$database-shm") |
        Where-Object { Test-Path -LiteralPath $_ -PathType Leaf }
    if ($sourceFiles.Count -lt 1) {
        throw "No operational database bundle files found."
    }
    $manifest = foreach ($source in $sourceFiles) {
        $target = Join-Path $Destination ([IO.Path]::GetFileName($source))
        Copy-Item -LiteralPath $source -Destination $target
        $sourceHash = (Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash
        $targetHash = (Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash
        if ($sourceHash -ne $targetHash) {
            throw "Database backup hash mismatch."
        }
        [ordered]@{
            name = [IO.Path]::GetFileName($source)
            size_bytes = (Get-Item -LiteralPath $target).Length
            sha256 = $targetHash
        }
    }
    $backupDatabase = Join-Path $Destination ([IO.Path]::GetFileName($database))
    $integrity = & $Python -c "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); print(c.execute('PRAGMA integrity_check').fetchone()[0]); c.close()" $backupDatabase
    if ($LASTEXITCODE -ne 0 -or $integrity.Trim() -ne "ok") {
        throw "Database backup integrity check failed."
    }
    [ordered]@{
        created_at = [DateTime]::UtcNow.ToString("o")
        source_path_redacted_to_filename = [IO.Path]::GetFileName($database)
        bundle_files = @($manifest)
        sqlite_integrity_check = $integrity.Trim()
    } | ConvertTo-Json -Depth 8 |
        Set-Content -LiteralPath (Join-Path $Destination "manifest.json") -Encoding UTF8
}

function Publish-LatestAcceptance {
    param(
        [string]$PayloadPath,
        [string]$ValidationReportPath,
        [string]$Destination
    )
    if (-not (Test-Path -LiteralPath $PayloadPath -PathType Leaf)) {
        throw "Cannot publish latest acceptance without the exact HTTP body."
    }
    if (-not (Test-Path -LiteralPath $ValidationReportPath -PathType Leaf)) {
        throw "Cannot publish latest acceptance without the validation report."
    }

    $report = Get-Content -LiteralPath $ValidationReportPath -Raw |
        ConvertFrom-Json
    if ($report.status -ne "PASS" -or -not $report.validated_exact_http_body) {
        throw "Latest acceptance can be published only from an exact-body PASS."
    }

    $payload = Get-Content -LiteralPath $PayloadPath -Raw |
        ConvertFrom-Json
    $payloadHash = (
        Get-FileHash -LiteralPath $PayloadPath -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    if ($payloadHash -ne ([string]$report.body_sha256).ToLowerInvariant()) {
        throw "Exact HTTP body hash does not match the validation report."
    }

    $responseGeneratedAt = $payload.generated_at
    $readinessStatus = $payload.readiness.status
    if (
        [string]::IsNullOrWhiteSpace([string]$responseGeneratedAt) -or
        [string]::IsNullOrWhiteSpace([string]$readinessStatus)
    ) {
        throw "Validated payload is missing generated_at or readiness.status."
    }

    $destinationDirectory = Split-Path -Parent $Destination
    New-Item -ItemType Directory -Path $destinationDirectory -Force |
        Out-Null
    $temporary = Join-Path $destinationDirectory (
        ".senior-analyst-live-latest.$([Guid]::NewGuid().ToString('N')).tmp"
    )
    try {
        $latest = [ordered]@{
            result = "PASS"
            response_generated_at = [string]$responseGeneratedAt
            full_payload_path = [IO.Path]::GetFullPath($PayloadPath)
            full_payload_sha256 = $payloadHash
            readiness_status = [string]$readinessStatus
        }
        $json = ($latest | ConvertTo-Json -Depth 4) + [Environment]::NewLine
        [IO.File]::WriteAllText(
            $temporary,
            $json,
            [Text.UTF8Encoding]::new($false)
        )
        if (Test-Path -LiteralPath $Destination -PathType Leaf) {
            [IO.File]::Replace($temporary, $Destination, $null)
        } else {
            [IO.File]::Move($temporary, $Destination)
        }
    } finally {
        if (Test-Path -LiteralPath $temporary -PathType Leaf) {
            Remove-Item -LiteralPath $temporary -Force
        }
    }
}

Push-Location $Repo
try {
    if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) {
        throw "Project Python runtime not found at .venv\Scripts\python.exe."
    }
    $branch = (git branch --show-current).Trim()
    $head = (git rev-parse HEAD).Trim()
    $base = (git merge-base HEAD origin/main).Trim()
    $remoteHead = (git rev-parse "origin/$ExpectedBranch").Trim()
    if ($branch -ne $ExpectedBranch -or $base -ne $ExpectedBase -or $head -ne $remoteHead) {
        throw "Git branch, base or pushed HEAD invariant failed."
    }
    $unexpected = @(git status --porcelain) |
        Where-Object { $_ -notmatch "^\?\? ai-trader-consumer-payload\.json$" }
    if ($unexpected.Count -gt 0) {
        throw "Git worktree contains unexpected changes."
    }
    Assert-PortFree -LocalPort $Port
    New-Item -ItemType Directory -Path $Output -Force | Out-Null

    # Settings() is intentionally used only during the future authorized LIVE run.
    $databasePath = (& $Python -c "from app.core.config import Settings; print(Settings().database_path.resolve())").Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($databasePath)) {
        throw "Unable to resolve the operational database through normal configuration."
    }
    Save-DatabaseBundle -DatabasePath $databasePath -Destination (Join-Path $Output "database-backup")

    $Process = Start-Process -FilePath $Python -WindowStyle Hidden -PassThru `
        -WorkingDirectory $Repo `
        -ArgumentList @("-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "$Port") `
        -RedirectStandardOutput $ServiceOut -RedirectStandardError $ServiceErr

    $readyDeadline = [DateTime]::UtcNow.AddSeconds(120)
    $healthUri = "http://127.0.0.1:$Port/health"
    do {
        if ($Process.HasExited) {
            throw "Service exited before readiness."
        }
        try {
            $health = Invoke-RestMethod -Method Get -Uri $healthUri -TimeoutSec 5
            $ready = $null -ne $health
        } catch {
            $ready = $false
        }
        if (-not $ready) {
            Start-Sleep -Milliseconds 500
        }
    } until ($ready -or [DateTime]::UtcNow -ge $readyDeadline)
    if (-not $ready) {
        throw "Service readiness timeout."
    }

    $RequestStartedAt = [DateTime]::UtcNow.ToString("o")
    $route = (
        "http://127.0.0.1:$Port/market-context/mnq" +
        "?refresh=force&view=consumer&audience=senior_analyst_v1"
    )
    $client = [Net.Http.HttpClient]::new()
    try {
        $client.Timeout = [TimeSpan]::FromSeconds($RequestTimeoutSeconds)
        $response = $client.GetAsync($route).GetAwaiter().GetResult()
        $body = $response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
        [IO.File]::WriteAllBytes($BodyPath, $body)
        $headerMap = [ordered]@{
            status_code = [int]$response.StatusCode
            reason_phrase = $response.ReasonPhrase
            request_uri = $route
            response_headers = [ordered]@{}
            content_headers = [ordered]@{}
        }
        foreach ($header in $response.Headers) {
            $headerMap.response_headers[$header.Key] = @($header.Value)
        }
        foreach ($header in $response.Content.Headers) {
            $headerMap.content_headers[$header.Key] = @($header.Value)
        }
        $headerMap | ConvertTo-Json -Depth 8 |
            Set-Content -LiteralPath $HeadersPath -Encoding UTF8
        if (-not $response.IsSuccessStatusCode) {
            throw "Production route returned HTTP $([int]$response.StatusCode)."
        }
        $bodyHash = (Get-FileHash -LiteralPath $BodyPath -Algorithm SHA256).Hash
        Set-Content -LiteralPath (Join-Path $Output "response-body.sha256") `
            -Value $bodyHash -Encoding ASCII
    } finally {
        $client.Dispose()
    }
} finally {
    if ($null -ne $Process) {
        Stop-ProcessTree -ParentId $Process.Id
        $Process.WaitForExit(15000) | Out-Null
    }
    Start-Sleep -Milliseconds 500
    $CleanupOk = -not [bool](
        Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    )
    Pop-Location
}

if (-not $CleanupOk) {
    throw "Process cleanup failed: port $Port is still listening."
}
if (-not (Test-Path -LiteralPath $BodyPath -PathType Leaf)) {
    throw "No HTTP response body was captured."
}

& $Python (Join-Path $Repo "scripts\validate_senior_analyst_payload.py") `
    --input $BodyPath `
    --headers $HeadersPath `
    --output $ReportPath `
    --request-started-at $RequestStartedAt `
    --require-live `
    --process-cleanup-ok
if ($LASTEXITCODE -ne 0) {
    throw "Senior Analyst LIVE validation failed. See $ReportPath"
}
Publish-LatestAcceptance `
    -PayloadPath $BodyPath `
    -ValidationReportPath $ReportPath `
    -Destination $LatestPath
Write-Output "LIVE validation passed: $ReportPath"
Write-Output "Latest acceptance published: $LatestPath"
