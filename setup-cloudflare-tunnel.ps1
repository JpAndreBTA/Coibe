param(
  [string]$TunnelName = "coibe-api",
  [string]$Hostname = "api.coibe.com.br"
)

$ErrorActionPreference = "Stop"
$env:CLOUDFLARED_NO_AUTOUPDATE = "true"

$cloudflared = Get-Command cloudflared -ErrorAction SilentlyContinue
if (-not $cloudflared) {
  throw "cloudflared nao encontrado. Instale pelo Cloudflare ou winget antes de continuar."
}

$cloudflaredDir = Join-Path $env:USERPROFILE ".cloudflared"
$certPath = Join-Path $cloudflaredDir "cert.pem"
if (-not (Test-Path -LiteralPath $certPath)) {
  Write-Host "Login Cloudflare necessario. Rode este comando, escolha coibe.com.br no navegador e execute este script novamente:"
  Write-Host "cloudflared tunnel login"
  exit 1
}

New-Item -ItemType Directory -Force -Path $cloudflaredDir | Out-Null

$existing = (& cloudflared tunnel list 2>&1 | Out-String) | Select-String -Pattern "\b$([regex]::Escape($TunnelName))\b"
if (-not $existing) {
  cloudflared tunnel create $TunnelName
}

$credentials = Get-ChildItem -LiteralPath $cloudflaredDir -Filter "*.json" |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

if (-not $credentials) {
  throw "Credenciais do tunnel nao encontradas em $cloudflaredDir."
}

$configPath = Join-Path $cloudflaredDir "config.yml"
$config = @"
tunnel: $TunnelName
credentials-file: $($credentials.FullName)

ingress:
  - hostname: $Hostname
    service: http://127.0.0.1:8000
  - service: http_status:404
"@

[System.IO.File]::WriteAllText($configPath, $config, [System.Text.UTF8Encoding]::new($false))

cloudflared tunnel route dns $TunnelName $Hostname

Write-Host "Tunnel configurado."
Write-Host "1) Rode a API: .\start-coibe-backend.ps1"
Write-Host "2) Em outro PowerShell, rode: cloudflared tunnel run $TunnelName"
Write-Host "3) URL publica da API: https://$Hostname"
