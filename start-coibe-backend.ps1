$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot
$BackendPort = 8000
$MonitorPidPath = Join-Path $PSScriptRoot "data\state\monitor.pid"

function Stop-PortProcess {
  param([int]$Port)
  Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue |
    Where-Object { $_.OwningProcess -ne 0 } |
    Select-Object -ExpandProperty OwningProcess -Unique |
    ForEach-Object {
      Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue
    }
}

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
