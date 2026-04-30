param(
  [string]$Database = "coibe",
  [string]$PostgresUser = "postgres",
  [string]$PostgresPassword = "coibe_local_2026",
  [string]$DbHost = "127.0.0.1",
  [int]$Port = 5432
)

$ErrorActionPreference = "Stop"

function Test-CommandAvailable {
  param([string]$Name)
  return [bool](Get-Command $Name -ErrorAction SilentlyContinue)
}

if (-not (Test-CommandAvailable "psql")) {
  Write-Warning "psql nao foi encontrado no PATH."
  Write-Host "Instale PostgreSQL/PostGIS e execute novamente."
  Write-Host "Windows com Chocolatey, em PowerShell como Administrador:"
  Write-Host "  choco install postgresql --params `"/Password:$PostgresPassword /Port:$Port`" --params-global -y"
  Write-Host "Depois habilite/instale PostGIS pelo instalador oficial ou StackBuilder e rode este script de novo."
  exit 1
}

$env:PGPASSWORD = $PostgresPassword

Write-Host "Verificando banco '$Database' em $DbHost`:$Port..."
$exists = (& psql -h $DbHost -p $Port -U $PostgresUser -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$Database';").Trim()

if ($exists -ne "1") {
  Write-Host "Criando banco '$Database'..."
  & psql -h $DbHost -p $Port -U $PostgresUser -d postgres --set ON_ERROR_STOP=1 -c "CREATE DATABASE $Database;"
}

Write-Host "Habilitando extensoes geoespaciais..."
& psql -h $DbHost -p $Port -U $PostgresUser -d $Database --set ON_ERROR_STOP=1 -c "CREATE EXTENSION IF NOT EXISTS postgis;"

Write-Host "Validando PostGIS..."
& psql -h $DbHost -p $Port -U $PostgresUser -d $Database --set ON_ERROR_STOP=1 -c "SELECT PostGIS_Full_Version();"

Write-Host ""
Write-Host "Configure no .env:"
Write-Host "COIBE_POSTGIS_ENABLED=true"
Write-Host "COIBE_POSTGIS_DATABASE_URL=postgresql://${PostgresUser}:${PostgresPassword}@${DbHost}:$Port/$Database"
