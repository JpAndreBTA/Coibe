param(
  [double]$MonitorIntervalMinutes = 15,
  [int]$MonitorPages = 10,
  [int]$MonitorPageSize = 50,
  [int]$BackendPort = 8000,
  [int]$FrontendPort = 5174
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$LogDir = Join-Path $Root "logs"
$MonitorPidPath = Join-Path $Root "data\state\monitor.pid"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

function Stop-PortProcess {
  param([int]$Port)
  Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue |
    Where-Object { $_.OwningProcess -ne 0 } |
    Select-Object -ExpandProperty OwningProcess -Unique |
    ForEach-Object {
      try { Stop-Process -Id $_ -Force -ErrorAction Stop } catch {}
    }
}

Stop-PortProcess -Port $BackendPort
Stop-PortProcess -Port $FrontendPort

if (Test-Path $MonitorPidPath) {
  $ExistingMonitorPid = Get-Content $MonitorPidPath -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($ExistingMonitorPid -match '^\d+$') {
    try { Stop-Process -Id ([int]$ExistingMonitorPid) -Force -ErrorAction Stop } catch {}
  }
  Remove-Item -LiteralPath $MonitorPidPath -Force -ErrorAction SilentlyContinue
}

$PythonExe = "python.exe"
$PythonPrefixArgs = @()
if (Get-Command py.exe -ErrorAction SilentlyContinue) {
  $PythonExe = "py.exe"
  $PythonPrefixArgs = @("-3.10")
}

$env:COIBE_AUTO_MONITOR_INTERVAL_MINUTES = "$MonitorIntervalMinutes"
$env:COIBE_AUTO_MONITOR_PAGES = "$MonitorPages"
$env:COIBE_AUTO_MONITOR_PAGE_SIZE = "$MonitorPageSize"
$env:COIBE_AUTO_MONITOR_API_BASE = "http://127.0.0.1:$BackendPort"

Start-Process -FilePath $PythonExe `
  -ArgumentList ($PythonPrefixArgs + @("-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", "$BackendPort")) `
  -WorkingDirectory $Root `
  -RedirectStandardOutput (Join-Path $LogDir "backend.out.log") `
  -RedirectStandardError (Join-Path $LogDir "backend.err.log") `
  -WindowStyle Hidden

Start-Sleep -Seconds 4

Start-Process -FilePath "npm.cmd" `
  -ArgumentList @("run", "dev", "--", "--host", "127.0.0.1", "--port", "$FrontendPort") `
  -WorkingDirectory $Root `
  -RedirectStandardOutput (Join-Path $LogDir "frontend.out.log") `
  -RedirectStandardError (Join-Path $LogDir "frontend.err.log") `
  -WindowStyle Hidden

Write-Host "COIBE.IA iniciado."
Write-Host "Frontend: http://127.0.0.1:$FrontendPort/"
Write-Host "Backend:  http://127.0.0.1:$BackendPort/docs"
Write-Host "Analise:  iniciada automaticamente pelo backend"
