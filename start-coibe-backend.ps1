$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

$env:COIBE_DATA_S3_SYNC = "false"
$env:COIBE_DATA_S3_WRITE_THROUGH = "false"
$env:COIBE_DATA_LOCAL_CACHE = "true"

py -3.10 -m uvicorn main:app --host 127.0.0.1 --port 8000
