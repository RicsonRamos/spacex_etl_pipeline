# scripts\generate_docs.ps1
# scripts/generate_docs.ps1

# Detectar diretório do script automaticamente
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$DbtPath = Join-Path $ProjectRoot "dbt_spacex"

# Verificar se dbt_spacex existe
if (-not (Test-Path $DbtPath)) {
    Write-Error "Diretório dbt_spacex não encontrado em: $DbtPath"
    exit 1
}

Write-Host "=== Gerando documentação dbt ===" -ForegroundColor Cyan
Write-Host "Path: $DbtPath"

Set-Location $DbtPath

# Verificar se dbt está disponível
try {
    $dbtVersion = dbt --version | Select-String "installed version" | Select-Object -First 1
    Write-Host "DBT encontrado: $dbtVersion" -ForegroundColor Green
} catch {
    Write-Error "dbt não encontrado no PATH. Verifique a instalação."
    exit 1
}

# Gerar docs
dbt docs generate

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n=== Documentação gerada com sucesso ===" -ForegroundColor Green
    Write-Host "Local: $DbtPath\target\index.html" -ForegroundColor Yellow
    Write-Host "Acesse: http://localhost:8081 (se container nginx estiver rodando)" -ForegroundColor Cyan
} else {
    Write-Error "Falha ao gerar documentação dbt"
    exit 1
}