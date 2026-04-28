param(
  [string]$Bucket = "coibe",
  [string]$EndpointUrl = "https://s3.g.s4.mega.io",
  [string]$Prefix = "data"
)

if (-not (Test-Path -LiteralPath "data")) {
  throw "A pasta data/ nao foi encontrada no diretorio atual."
}

$destination = "s3://$Bucket/$Prefix"
aws s3 sync "data/" $destination --endpoint-url $EndpointUrl --delete

Write-Host "Dados sincronizados em $destination"
