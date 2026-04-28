$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

if (-not $env:COIBE_DATA_S3_SYNC) {
  $env:COIBE_DATA_S3_SYNC = "true"
}
if (-not $env:COIBE_DATA_S3_WRITE_THROUGH) {
  $env:COIBE_DATA_S3_WRITE_THROUGH = "true"
}
if (-not $env:COIBE_DATA_LOCAL_CACHE) {
  $env:COIBE_DATA_LOCAL_CACHE = "false"
}

py -3.10 -m uvicorn main:app --host 127.0.0.1 --port 8000
