param(
  [Parameter(Mandatory = $true)]
  [string]$Bucket,

  [string]$EndpointUrl = "",

  [string]$ApiBaseUrl = ""
)

if ($ApiBaseUrl) {
  $env:VITE_API_BASE_URL = $ApiBaseUrl
}

npm ci
npm run build

$args = @("s3", "sync", "dist/", "s3://$Bucket", "--delete")
if ($EndpointUrl) {
  $args += @("--endpoint-url", $EndpointUrl)
}

aws @args
