param(
  [string]$Bucket = "coibe",
  [string]$EndpointUrl = "https://s3.g.s4.mega.io",
  [string]$Prefix = "data",
  [string]$Region = "eu-central-1"
)

if (-not (Test-Path -LiteralPath "data")) {
  throw "A pasta data/ nao foi encontrada no diretorio atual."
}

$env:COIBE_UPLOAD_BUCKET = $Bucket
$env:COIBE_UPLOAD_ENDPOINT_URL = $EndpointUrl
$env:COIBE_UPLOAD_PREFIX = $Prefix
$env:COIBE_UPLOAD_REGION = $Region

@'
import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv(".env")

bucket = os.environ["COIBE_UPLOAD_BUCKET"]
endpoint_url = os.environ["COIBE_UPLOAD_ENDPOINT_URL"]
prefix = os.environ["COIBE_UPLOAD_PREFIX"].strip("/")
region = os.environ["COIBE_UPLOAD_REGION"]

s3 = boto3.client("s3", endpoint_url=endpoint_url, region_name=region)

root = Path("data")
files = [path for path in root.rglob("*") if path.is_file()]
uploaded = 0
for path in files:
    relative = path.relative_to(root).as_posix()
    key = f"{prefix}/{relative}" if prefix else relative
    content_type = "application/json; charset=utf-8" if path.suffix == ".json" else "text/plain; charset=utf-8"
    s3.upload_file(
        str(path),
        bucket,
        key,
        ExtraArgs={"ContentType": content_type},
    )
    uploaded += 1
    if uploaded % 250 == 0:
        print(f"{uploaded}/{len(files)} arquivos enviados...")

print(f"Dados sincronizados em s3://{bucket}/{prefix}: {uploaded} arquivo(s).")
'@ | py -3.10 -
