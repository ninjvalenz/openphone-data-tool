param(
    [int]$LocalPort = 8080,
    [string]$Label = "local_calls",
    [string]$Events = "call.completed"
)

$ErrorActionPreference = "Stop"

function Get-DotEnvValue {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Key
    )

    if (-not (Test-Path $Path)) { return $null }

    $escapedKey = [Regex]::Escape($Key)
    foreach ($line in Get-Content -Path $Path) {
        $trimmed = $line.Trim().TrimStart([char]0xFEFF)
        if (-not $trimmed -or $trimmed.StartsWith("#")) { continue }
        if ($trimmed -match "^\s*$escapedKey=(.*)$") {
            $value = $Matches[1].Trim()
            if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
                $value = $value.Substring(1, $value.Length - 2)
            }
            return $value
        }
    }

    return $null
}

function Set-DotEnvValue {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Key,
        [Parameter(Mandatory = $true)][string]$Value
    )

    $lines = @()
    if (Test-Path $Path) { $lines = Get-Content -Path $Path }

    $updated = $false
    for ($i = 0; $i -lt $lines.Count; $i++) {
        $trimmed = $lines[$i].TrimStart([char]0xFEFF).TrimStart()
        if ($trimmed.StartsWith("$Key=", [System.StringComparison]::Ordinal)) {
            $lines[$i] = "$Key=$Value"
            $updated = $true
        }
    }

    if (-not $updated) { $lines += "$Key=$Value" }

    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllLines($Path, $lines, $utf8NoBom)
}

$projectRoot = if ($PSScriptRoot) { $PSScriptRoot } else { (Get-Location).Path }
Set-Location $projectRoot

$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    throw "Project virtualenv Python not found at $python. Create .venv first."
}
Write-Host "Using Python: $python"
& $python -c "import asyncio; print(asyncio.__file__)" | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Python environment failed importing asyncio."
}

if (-not (Get-Command ngrok -ErrorAction SilentlyContinue)) {
    throw "ngrok not found in PATH."
}

$envFile = Join-Path $projectRoot ".env"
if (-not (Test-Path $envFile)) {
    throw ".env file not found at $envFile"
}

$apiKey = Get-DotEnvValue -Path $envFile -Key "OPENPHONE_API_KEY"
if ([string]::IsNullOrWhiteSpace($apiKey)) {
    throw "OPENPHONE_API_KEY is missing in .env"
}
$env:OPENPHONE_API_KEY = $apiKey

# Restart ngrok
Get-Process ngrok -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Milliseconds 500
Start-Process -FilePath "ngrok" -ArgumentList @("http", "$LocalPort") -WindowStyle Minimized | Out-Null

# Wait for ngrok URL
$publicUrl = $null
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 1
    try {
        $tunnels = Invoke-RestMethod -Uri "http://127.0.0.1:4040/api/tunnels" -TimeoutSec 2
        $httpsTunnel = $tunnels.tunnels | Where-Object { $_.proto -eq "https" } | Select-Object -First 1
        if ($httpsTunnel) {
            $publicUrl = [string]$httpsTunnel.public_url
            break
        }
    } catch { }
}
if (-not $publicUrl) {
    throw "Could not get ngrok URL from http://127.0.0.1:4040/api/tunnels"
}

Write-Host "ngrok URL: $publicUrl"

# Delete previous calls webhook URL from .env (if present)
$oldBaseUrl = Get-DotEnvValue -Path $envFile -Key "OPENPHONE_WEBHOOK_BASE_URL"
if ($oldBaseUrl) {
    & $python -m jobs.setup_webhook --type calls --base-url $oldBaseUrl --label $Label --delete-only | Out-Null
}

# Recreate local calls webhook on current ngrok URL
$setupOut = & $python -m jobs.setup_webhook --type calls --events $Events --base-url $publicUrl --label $Label --delete-existing
if ($LASTEXITCODE -ne 0) {
    throw "Webhook setup failed."
}

$webhook = $setupOut | Out-String | ConvertFrom-Json
$key = [string]$webhook.key
if ([string]::IsNullOrWhiteSpace($key)) {
    throw "Webhook created but no signing key returned."
}

# Reset signing secret + base URL in env and .env
$env:OPENPHONE_WEBHOOK_BASE_URL = $publicUrl
$env:OPENPHONE_WEBHOOK_SIGNING_SECRET_CALLS = $key
Set-DotEnvValue -Path $envFile -Key "OPENPHONE_WEBHOOK_BASE_URL" -Value $publicUrl
Set-DotEnvValue -Path $envFile -Key "OPENPHONE_WEBHOOK_SIGNING_SECRET_CALLS" -Value $key

Write-Host ""
Write-Host "Done."
Write-Host "Webhook ID: $($webhook.id)"
Write-Host "Webhook URL: $($webhook.url)"
Write-Host "Signing key (OPENPHONE_WEBHOOK_SIGNING_SECRET_CALLS): $key"
try {
    $keyBytes = [Convert]::FromBase64String($key)
    $sha = [System.Security.Cryptography.SHA256]::Create()
    $fingerprint = -join ($sha.ComputeHash($keyBytes) | ForEach-Object { $_.ToString("x2") })
    Write-Host "Signing key fingerprint (sha256, first 12): $($fingerprint.Substring(0,12))"
} catch {
    Write-Host "Signing key fingerprint: unavailable (invalid base64?)"
}
Write-Host "Signature key reset in current shell + .env"
Write-Host ""
Write-Host "Starting calls receiver (Ctrl+C to stop)..."
& $python -u -m events.op_new_calls_receiver
