#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Script Docker-first - SpaceX ETL Pipeline
# -----------------------------

PROJECT_ROOT=$(pwd)
DBT_PATH="$PROJECT_ROOT/dbt_spacex"
START_TIME=$(date +"%Y-%m-%d %H:%M:%S")

GREEN="\e[32m"
CYAN="\e[36m"
RED="\e[31m"
YELLOW="\e[33m"
RESET="\e[0m"

function step() { echo -e "${CYAN}=== $1 ===${RESET}"; }
function success() { echo -e "${GREEN}✅ $1${RESET}"; }
function warning() { echo -e "${YELLOW}⚠️  $1${RESET}"; }
function error() { echo -e "${RED}❌ $1${RESET}"; }

echo ""
step "INICIANDO TESTE COMPLETO - SpaceX ETL Pipeline"
echo "Projeto: $PROJECT_ROOT"
echo "DBT Path: $DBT_PATH"
echo "Início: $START_TIME"
echo ""

# ==================== TESTE 1: Arquivos ====================
step "TESTE 1: Verificando estrutura de arquivos"
required_files=(
    "docker-compose.yml"
    ".env"
    "dbt_spacex/dbt_project.yml"
    "dbt_spacex/models/marts/fct_launches_performance.sql"
    "dbt_spacex/models/marts/fct_space_weather_impact.sql"
    "dbt_spacex/models/marts/fct_spacex_launch_roi.sql"
    "dbt_spacex/models/staging/spacex/stg_spacex__launches.sql"
    "dbt_spacex/models/staging/nasa/stg_nasa__solar_events.sql"
    ".github/workflows/ci.yml"
    ".github/workflows/cd.yml"
    "scripts/health_check.ps1"
)

missing_files=()
for f in "${required_files[@]}"; do
    if [[ ! -f "$PROJECT_ROOT/$f" ]]; then
        error "Arquivo ausente: $f"
        missing_files+=("$f")
    fi
done

if [[ ${#missing_files[@]} -eq 0 ]]; then
    success "Todos os arquivos obrigatórios presentes"
else
    error "Faltam ${#missing_files[@]} arquivo(s)"
    exit 1
fi
echo ""

# ==================== TESTE 2: Variáveis de ambiente ====================
step "TESTE 2: Verificando variáveis de ambiente"
if [[ ! -f "$PROJECT_ROOT/.env" ]]; then
    error ".env não encontrado"
    exit 1
fi

required_vars=("POSTGRES_USER" "POSTGRES_PASSWORD" "POSTGRES_DB" "NASA_API_KEY")
missing_vars=()
for var in "${required_vars[@]}"; do
    if ! grep -q "^$var=" "$PROJECT_ROOT/.env"; then
        error "Variável ausente: $var"
        missing_vars+=("$var")
    fi
done

if [[ ${#missing_vars[@]} -eq 0 ]]; then
    success "Todas as variáveis obrigatórias presentes"
else
    error "Faltam ${#missing_vars[@]} variável(is)"
    exit 1
fi
echo ""

# ==================== TESTE 3: Docker Compose ====================
step "TESTE 3: Validando docker-compose.yml"
if docker compose config >/dev/null 2>&1; then
    success "docker-compose.yml válido"
else
    error "docker-compose.yml inválido"
    exit 1
fi
echo ""

# ==================== TESTE 4: Containers ====================
step "TESTE 4: Verificando containers"
required_containers=("spacex_postgres" "airflow_webserver" "airflow_scheduler" "metabase")
running_containers=$(docker ps --format "{{.Names}}")

for c in "${required_containers[@]}"; do
    if echo "$running_containers" | grep -q "$c"; then
        success "Container rodando: $c"
    else
        warning "Container não encontrado: $c"
        echo "   Inicie com: docker compose up -d"
    fi
done
echo ""

# ==================== TESTE 5: PostgreSQL ====================
step "TESTE 5: Testando conexão PostgreSQL"
max_retries=5
retry=0
connected=false
while [[ $retry -lt $max_retries ]]; do
    if docker exec spacex_postgres pg_isready -U admin -d spacex_db | grep -q "accepting connections"; then
        success "PostgreSQL conectado"
        connected=true
        break
    else
        warning "Tentativa $((retry+1)) falhou, aguardando..."
        sleep 2
    fi
    retry=$((retry+1))
done
if [[ $connected != true ]]; then
    error "Não foi possível conectar ao PostgreSQL"
    exit 1
fi
echo ""

# ==================== TESTE 6-8: DBT ====================
step "TESTE 6-8: Executando DBT (deps, compile, test)"
docker run --rm -v "$DBT_PATH:/dbt" -w /dbt --network spacex_etl_pipeline_default dbt:latest bash -c "
    set -e
    python -m dbt deps
    python -m dbt compile --target docker
    python -m dbt test --target docker
"
success "DBT executado com sucesso"
echo ""

# ==================== TESTE 9: Ingestion engine ====================
step "TESTE 9: Testando ingestion engine"
docker run --rm --network spacex_etl_pipeline_default \
    -e DATABASE_URL="postgresql://admin:PASSWORD@spacex_postgres:5432/spacex_db" \
    -e NASA_API_KEY=$(grep NASA_API_KEY .env | cut -d '=' -f2) \
    spacex_etl_pipeline-ingestion_engine:latest \
    python /app/main.py --test-mode
success "Ingestion engine testado"
echo ""

# ==================== TESTE 10: Endpoints HTTP ====================
step "TESTE 10: Verificando endpoints"
declare -A endpoints=(
    [Airflow]="http://localhost:8080/health"
    [Metabase]="http://localhost:3000/api/health"
    [DBT_Docs]="http://localhost:8081"
)
for name in "${!endpoints[@]}"; do
    if curl -s --head --fail "${endpoints[$name]}" >/dev/null; then
        success "$name: OK"
    else
        warning "$name: Indisponível"
    fi
done
echo ""

# ==================== RESUMO ====================
END_TIME=$(date +"%Y-%m-%d %H:%M:%S")
step "RESUMO DO TESTE"
echo "Início: $START_TIME"
echo "Fim:    $END_TIME"
echo ""
success "Pipeline SpaceX ETL validado com sucesso!"
echo "Próximos passos:"
echo "  - Acesse Airflow:    http://localhost:8080"
echo "  - Acesse Metabase:   http://localhost:3000"
echo "  - Acesse DBT Docs:   http://localhost:8081"
echo "  - Execute a DAG:     spacex_full_pipeline"