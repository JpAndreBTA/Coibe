param(
  [string]$Database = "",
  [string]$PostgresUser = "",
  [string]$PostgresPassword = "",
  [string]$DbHost = "",
  [int]$Port = 0
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path

function Import-CoibeDotEnv {
  $envPath = Join-Path $Root ".env"
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

function Test-CommandAvailable {
  param([string]$Name)
  return [bool](Get-Command $Name -ErrorAction SilentlyContinue)
}

function Start-PostgresServiceIfPresent {
  $service = Get-Service -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match '^postgresql' -or $_.DisplayName -match 'PostgreSQL|Postgres' } |
    Sort-Object Name -Descending |
    Select-Object -First 1
  if ($service -and $service.Status -ne "Running") {
    Write-Host "Iniciando servico PostgreSQL: $($service.Name)"
    Start-Service -Name $service.Name
    $service.WaitForStatus("Running", "00:00:25")
  }
}

Import-CoibeDotEnv

$databaseUrl = $env:COIBE_POSTGIS_DATABASE_URL
if ($databaseUrl) {
  $uri = [Uri]$databaseUrl
  $userInfo = $uri.UserInfo -split ":", 2
  if (-not $PostgresUser -and $userInfo.Count -ge 1) { $PostgresUser = [Uri]::UnescapeDataString($userInfo[0]) }
  if (-not $PostgresPassword -and $userInfo.Count -ge 2) { $PostgresPassword = [Uri]::UnescapeDataString($userInfo[1]) }
  if (-not $DbHost) { $DbHost = $uri.Host }
  if (-not $Port -or $Port -le 0) { $Port = $uri.Port }
  if (-not $Database) { $Database = $uri.AbsolutePath.Trim("/") }
}

if (-not $Database) { $Database = "coibe" }
if (-not $PostgresUser) { $PostgresUser = "postgres" }
if (-not $PostgresPassword) { $PostgresPassword = $env:PGPASSWORD }
if (-not $PostgresPassword) { $PostgresPassword = $env:COIBE_POSTGRES_PASSWORD }
if (-not $PostgresPassword) { $PostgresPassword = "coibe_local_2026" }
if (-not $DbHost) { $DbHost = "127.0.0.1" }
if (-not $Port -or $Port -le 0) { $Port = 5432 }

Start-PostgresServiceIfPresent

if (-not (Test-CommandAvailable "psql")) {
  Write-Warning "psql nao foi encontrado no PATH."
  Write-Host "Instale PostgreSQL/PostGIS e execute novamente."
  Write-Host "Windows com Chocolatey, em PowerShell como Administrador:"
  Write-Host "  choco install postgresql --params `"/Password:SUA_SENHA /Port:$Port`" --params-global -y"
  Write-Host "Depois habilite/instale PostGIS pelo instalador oficial ou StackBuilder e rode este script de novo."
  exit 1
}

$env:PGPASSWORD = $PostgresPassword

Write-Host "Verificando banco '$Database' em $DbHost`:$Port..."
$exists = (& psql -h $DbHost -p $Port -U $PostgresUser -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$Database';")
$exists = ($exists | Select-Object -First 1)

if (($exists -as [string]).Trim() -ne "1") {
  Write-Host "Criando banco '$Database'..."
  & psql -h $DbHost -p $Port -U $PostgresUser -d postgres --set ON_ERROR_STOP=1 -c "CREATE DATABASE $Database;"
}

Write-Host "Habilitando extensoes geoespaciais..."
& psql -h $DbHost -p $Port -U $PostgresUser -d $Database --set ON_ERROR_STOP=1 -c "CREATE EXTENSION IF NOT EXISTS postgis;"

Write-Host "Validando PostGIS..."
& psql -h $DbHost -p $Port -U $PostgresUser -d $Database --set ON_ERROR_STOP=1 -c "SELECT PostGIS_Full_Version();"

Write-Host ""
Write-Host "PostGIS pronto para o COIBE."
Write-Host "COIBE_POSTGIS_DATABASE_URL=postgresql://${PostgresUser}:***@${DbHost}:$Port/$Database"
