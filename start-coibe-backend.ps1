$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot
$BackendPort = 8000
$MonitorPidPath = Join-Path $PSScriptRoot "data\state\monitor.pid"

function Import-CoibeDotEnv {
  $envPath = Join-Path $PSScriptRoot ".env"
  if (-not (Test-Path -LiteralPath $envPath)) { return }
  Get-Content -LiteralPath $envPath | ForEach-Object {
    $line = $_.Trim()
    if (-not $line -or $line.StartsWith("#") -or $line -notmatch "=") { return }
    $key, $value = $line -split "=", 2
    $key = $key.Trim()
    $value = $value.Trim().Trim('"').Trim("'")
    if ($key) { [Environment]::SetEnvironmentVariable($key, $value, "Process") }
  }
}

function Start-CoibePostgres {
  $configuredService = $env:COIBE_POSTGRES_SERVICE_NAME
  $service = $null
  if ($configuredService) {
    $service = Get-Service -Name $configuredService -ErrorAction SilentlyContinue
  }
  if (-not $service) {
    $service = Get-Service -ErrorAction SilentlyContinue |
      Where-Object { $_.Name -match '^postgresql' -or $_.DisplayName -match 'PostgreSQL|Postgres' } |
      Sort-Object Name -Descending |
      Select-Object -First 1
  }
  if ($service -and $service.Status -ne "Running") {
    Write-Host "Iniciando PostgreSQL: $($service.Name)"
    Start-Service -Name $service.Name
    $service.WaitForStatus("Running", "00:00:25")
  }
  if (Test-Path -LiteralPath (Join-Path $PSScriptRoot "setup-postgis-local.ps1")) {
    powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "setup-postgis-local.ps1")
  }
}

function Stop-PortProcess {
  param([int]$Port)
  Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue |
    Where-Object { $_.OwningProcess -ne 0 } |
    Select-Object -ExpandProperty OwningProcess -Unique |
    ForEach-Object {
      Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue
    }
}

Import-CoibeDotEnv
Start-CoibePostgres

Stop-PortProcess -Port $BackendPort

Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -match 'local_monitor\.py|coibe_prompt_monitor\.py' } |
  ForEach-Object {
    Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
  }

if (Test-Path -LiteralPath $MonitorPidPath) {
  $monitorPid = Get-Content -LiteralPath $MonitorPidPath -Raw
  if ($monitorPid -match '^\s*\d+\s*$') {
    Stop-Process -Id ([int]$monitorPid) -Force -ErrorAction SilentlyContinue
  }
  Remove-Item -LiteralPath $MonitorPidPath -Force -ErrorAction SilentlyContinue
}

$env:COIBE_DATA_S3_SYNC = "false"
$env:COIBE_DATA_S3_WRITE_THROUGH = "false"
$env:COIBE_DATA_LOCAL_CACHE = "true"
$env:COIBE_AUTO_MONITOR = "false"

py -3.10 -m uvicorn main:app --host 127.0.0.1 --port $BackendPort
