# SpaceX Production ETL Pipeline

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.5%2B-red)](https://airflow.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.5.0-orange)](https://www.getdbt.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14%2B-blue)](https://postgresql.org)
[![Metabase](https://img.shields.io/badge/Metabase-Latest-green)](https://metabase.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue)](https://docker.com)
[![CI/CD](https://github.com/RicsonRamos/spacex_etl_pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/RicsonRamos/spacex_etl_pipeline/actions)
[![Coverage](https://img.shields.io/badge/coverage-82%25-brightgreen)](https://github.com/RicsonRamos/spacex_etl_pipeline)

> Pipeline de dados completo para ingestão, transformação e visualização de missões espaciais da SpaceX e NASA. Arquitetura Medallion (Bronze, Silver, Gold) com CI/CD automatizado e 82% de cobertura de testes.

---

## Índice

- [SpaceX Production ETL Pipeline](#spacex-production-etl-pipeline)
  - [Índice](#índice)
  - [Visão Geral](#visão-geral)
  - [Arquitetura](#arquitetura)
  - [Stack Tecnológico](#stack-tecnológico)
  - [Estrutura de Dados (Medallion)](#estrutura-de-dados-medallion)
    - [Bronze (Raw Data)](#bronze-raw-data)
    - [Silver (Staging)](#silver-staging)
    - [Gold (Marts)](#gold-marts)
  - [CI/CD e Qualidade de Código](#cicd-e-qualidade-de-código)
    - [GitHub Actions Workflow](#github-actions-workflow)
    - [Cobertura de Testes](#cobertura-de-testes)
    - [Testes Unitários](#testes-unitários)
  - [Decisões de Design](#decisões-de-design)
    - [A. Ciclo de Vida do dbt em Contêineres](#a-ciclo-de-vida-do-dbt-em-contêineres)
    - [B. Governança com Surrogate Keys](#b-governança-com-surrogate-keys)
    - [C. Testes Automatizados com Pytest](#c-testes-automatizados-com-pytest)
  - [KPIs de Negócio (Camada Gold)](#kpis-de-negócio-camada-gold)
  - [Governança e Qualidade](#governança-e-qualidade)
    - [Protocolo de Failsafe](#protocolo-de-failsafe)
    - [Exemplo de Testes (`schema.yml`)](#exemplo-de-testes-schemayml)
  - [Observabilidade](#observabilidade)
    - [Logger Estruturado JSON](#logger-estruturado-json)
    - [Alertas e SLA](#alertas-e-sla)
  - [Como Executar](#como-executar)
    - [1. Pré-requisitos](#1-pré-requisitos)
    - [2. Configuração (.env)](#2-configuração-env)
    - [3. Subir Infraestrutura](#3-subir-infraestrutura)
    - [4. Executar Testes Localmente](#4-executar-testes-localmente)
    - [5. Executar Pipeline](#5-executar-pipeline)
    - [6. Acessar Dashboards](#6-acessar-dashboards)
  - [Estrutura de Pastas](#estrutura-de-pastas)
  - [Roadmap](#roadmap)
  - [Contribuição](#contribuição)
  - [Licença](#licença)
  - [Autor](#autor)

---

## Visão Geral

Este projeto implementa um pipeline de dados robusto que transforma dados brutos de APIs espaciais em inteligência de negócio acionável. A arquitetura segue o padrão **Medallion (Bronze, Silver, Gold)**, garantindo qualidade, rastreabilidade e governança em cada camada.

**Diferenciais implementados:**
-  **CI/CD completo** com GitHub Actions (testes automatizados + cobertura 82%)
-  **Testes unitários robustos** com pytest (58 testes passando)
-  **Ingestão automatizada** de dados da SpaceX e NASA
-  **Qualidade garantida** através de testes rigorosos com dbt
-  **Dashboards em tempo real** via Metabase
-  **Observabilidade completa** com logs estruturados e alertas

---

## Arquitetura

 Aqui está o diagrama de arquitetura completo e formatado corretamente:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   APIs Externas │     │   Apache Airflow│     │    PostgreSQL   │
│  SpaceX / NASA  │────▶│   (Orquestração)│────▶│   (Data Warehouse)│
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                │                          │
                                ▼                          ▼
                       ┌─────────────────┐          ┌─────────────────┐
                       │      dbt        │          │    Metabase     │
                       │ (Transformação) │          │   (Dashboards)  │
                       └─────────────────┘          └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │  GitHub Actions │
                       │   (CI/CD Tests) │
                       └─────────────────┘
```

**Fluxo de Dados:**
1. **Bronze** → Ingestão raw via Python (APIs SpaceX/NASA)
2. **Silver** → Limpeza, padronização e surrogate keys (dbt)
3. **Gold** → Agregações e KPIs prontos para BI (dbt)
4. **CI/CD** → Testes automatizados em cada push/PR

---

## Stack Tecnológico

| Componente | Tecnologia | Versão | Propósito |
|------------|-----------|---------|-----------|
| **Orquestração** | Apache Airflow | 2.5+ | Agendamento e monitoramento |
| **Transformação** | dbt (data build tool) | 1.5.0 | Modelagem SQL + testes |
| **Warehouse** | PostgreSQL | 14+ | Armazenamento estruturado |
| **Visualização** | Metabase | Latest | Dashboards self-service |
| **Infraestrutura** | Docker + Compose | 20+ | Containerização total |
| **Ingestão** | Python + Requests | 3.9+ | ETL customizado |
| **CI/CD** | GitHub Actions | - | Testes automatizados |
| **Testes** | pytest + pytest-cov | - | 58 testes unitários |

---

**Contrato de Dados: Inputs & Outputs**

Esta seção define explicitamente os inputs, outputs e efeitos colaterais da pipeline — essencial para produção, auditoria e manutenção.

**Fluxo Geral**

```bash
Params + ENV
   ↓
validate_environment
   ↓
ingest_data
   ↓
PostgreSQL (Bronze)
   ↓
dbt (Silver → Gold)
   ↓
Metabase / DAG downstream
```

**DAG: spacex_full_pipeline**

**Inputs da DAG**

Parâmetros (Airflow)
| Parâmetro  | Tipo   | Descrição    |
| ---------- | ------ | ------------ |
| start_date | string | Data inicial |
| end_date   | string | Data final   |
| api_source | string | URL da API   |

Variáveis de Ambiente
| Variável          | Obrigatória |
| ----------------- | ----------- |
| POSTGRES_USER     | ✅           |
| POSTGRES_PASSWORD | ✅           |
| POSTGRES_DB       | ✅           |
| NASA_API_KEY      | ✅           |
| ALERT_EMAIL       | ❌           |
| SLACK_WEBHOOK_URL | ❌           |

## Tasks da Pipeline

| Task | Inputs | Processo/Validação | Output |
|------|--------|-------------------|--------|
| **validate_environment** | ENV vars, params | Validação de configurações | OK / erro |
| **ingest_data** (Docker) | API SpaceX, Datas, DATABASE_URL | Extract → Transform → Load | `bronze.spacex_launches` |
| **dbt_deps** | — | Instalação de dependências | Pacotes dbt instalados |
| **dbt_freshness** | Tabelas Bronze | Valida atualização dos dados | Relatório de frescor |
| **dbt_run** | Bronze/Silver | Transformação: Bronze → Silver → Gold | Tabelas Silver/Gold |
| **dbt_test** | Modelos dbt | Valida qualidade dos dados | passed / failed |
| **dbt_docs_generate** | Modelos dbt | Gera documentação | Site estático em `/target` |
| **trigger_other_pipeline** | DAG concluída | Dispara pipeline downstream | Próxima DAG executada |

---

## Scripts Python

### `main.py`
| Aspecto | Descrição |
|---------|-----------|
| **Inputs** | APIs, Config, ENV |
| **Output** | Dados no PostgreSQL |

### `api_client.py`
| Aspecto | Descrição |
|---------|-----------|
| **Responsabilidade** | Requisições HTTP com retry |
| **Output** | DataFrames |

### `extractors`
| Aspecto | Descrição |
|---------|-----------|
| **Output** | DataFrames processados |

### `loaders`
| Aspecto | Descrição |
|---------|-----------|
| **Output** | Persistência no banco |

---

## Camadas de Dados

| Camada | Descrição |
|--------|-----------|
| **Bronze** | Dados crus da API |
| **Silver** | Dados limpos e padronizados |
| **Gold** | KPIs e métricas de negócio |

---

## Observabilidade

- **Logs JSON** estruturados
- **Alertas** Slack/Email
- **SLA** + retries exponenciais

---

## Matriz de Falhas

| Etapa | Impacto | Mitigação |
|-------|---------|-----------|
| `ingest` | Sem dados | Retries + alerta |
| `dbt_test` | Pipeline bloqueado | Falha crítica, não prossegue |
| `freshness` | Dados inválidos | Alerta + verificação manual |



## Estrutura de Dados (Medallion)

### Bronze (Raw Data)
Dados crus ingeridos via API, persistidos sem alteração de schema.

```sql
-- Exemplo: raw_launches
SELECT * FROM bronze.spacex_launches;
```

### Silver (Staging)
Limpeza de tipos, padronização UTC, criação de `launch_key` e surrogate keys.

```sql
-- Exemplo: stg_launches
SELECT 
    launch_key,
    mission_name,
    launch_date_utc,
    rocket_type,
    launch_status
FROM silver.stg_launches;
```

### Gold (Marts)
Tabelas agregadas para KPIs prontas para consumo.

```sql
-- Exemplo: fct_launches_gold
SELECT 
    launch_status, 
    COUNT(*) as total,
    AVG(payload_mass_kg) as avg_payload
FROM gold.fct_launches
GROUP BY launch_status;
```

---

## CI/CD e Qualidade de Código

### GitHub Actions Workflow

O projeto possui **CI/CD completo** configurado em `.github/workflows/ci.yml`:

```yaml
name: CI - Test

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: |
          pip install pytest pytest-cov pandas requests sqlalchemy python-dotenv
      - name: Run tests + coverage
        env:
          PYTHONPATH: .
        run: |
          python -m pytest ingestion_engine/tests/ -v \
            --cov=src \
            --cov=config \
            --cov=main \
            --cov=ingestion_engine/utils \
            --cov-report=term-missing \
            --cov-fail-under=80
```

**Gatilhos:**
- Push para `main` ou `develop`
- Pull requests para `main`

### Cobertura de Testes

| Módulo | Cobertura | Status |
|--------|-----------|--------|
| `main.py` | **100%** | ✅ |
| `src/extractors/concrete_extractors.py` | **100%** | ✅ |
| `src/utils/logger.py` | 83% | ✅ |
| `src/interfaces/extractor_interface.py` | 83% | ✅ |
| `ingestion_engine/utils/api_client.py` | 72% | ✅ |
| **TOTAL** | **82%** | ✅ (meta: 80%) |

### Testes Unitários

**58 testes implementados** cobrindo:

- **API Client** (`test_api_client.py`): Mocking de APIs SpaceX/NASA
- **API Extractor** (`test_api_extractor.py`): Extração com retry, rate limiting, erros HTTP
- **Ingestion** (`test_ingestion.py`): Pipeline completo Bronze→Silver
- **Data Quality** (`test_ingestion_quality.py`): Validações de DataFrame
- **Main Orchestrator** (`test_main.py`): Mocks de dependências, preflight checks

**Exemplo de teste com mocks:**
```python
@pytest.fixture
def mock_all_dependencies():
    with patch('main.PostgresLoader') as mock_postgres_cls, \
         patch('main.AlertSystem') as mock_alert_cls, \
         patch('main.APIExtractor') as mock_extractor_cls, \
         patch('main.get_endpoints_config') as mock_get_config:
        yield {
            'postgres_cls': mock_postgres_cls,
            'alert_cls': mock_alert_cls,
            'extractor_cls': mock_extractor_cls,
            'get_config': mock_get_config
        }
```

---

## Decisões de Design

### A. Ciclo de Vida do dbt em Contêineres
**Desafio:** Sincronizar código dbt no Host (Windows/VS Code) com Airflow sem perder dependências.

**Solução:** Bind Mounts + task `dbt_deps` obrigatória na DAG.

**Benefício:** Garante que `packages.yml` seja resolvido antes de qualquer execução.

### B. Governança com Surrogate Keys
**Desafio:** IDs de APIs externas podem ser instáveis ou duplicados entre fontes.

**Solução:** Surrogate Keys na camada Silver usando `dbt_utils.generate_surrogate_key`.

**Garantia:** Integridade referencial absoluta independente da fonte original.

### C. Testes Automatizados com Pytest
**Desafio:** Garantir qualidade do código Python sem testes manuais.

**Solução:** 
- 58 testes unitários com pytest
- Mocks de dependências externas (Postgres, APIs, Alertas)
- Cobertura mínima de 80% via pytest-cov
- Integração com GitHub Actions para validação em cada PR

**Benefício:** Código robusto, refatorável e com regressões detectadas automaticamente.

---

## KPIs de Negócio (Camada Gold)

| KPI | Descrição | Fonte |
|-----|-----------|-------|
| **Taxa de Sucesso por Ano** | % de lançamentos bem-sucedidos | `fct_launches` |
| **Eficiência de Custo** | Orçamento NASA × Volume SpaceX | `fct_budget_efficiency` |
| **Densidade de Lançamentos** | Missões por `rocket_type` e `launch_pad` | `fct_launch_density` |
| **Tempo Médio entre Falhas** | MTBF por família de foguetes | `fct_reliability` |

---

## Governança e Qualidade

### Protocolo de Failsafe
O pipeline interrompe automaticamente se:

- `dbt_test` falhar → Impede dados "sujos" de chegar ao Gold
- `source_freshness` falhar → Evita dashboards defasados
- **Testes unitários falharem no CI/CD** → Impede deploy de código quebrado

### Exemplo de Testes (`schema.yml`)

```yaml
models:
  - name: fct_launches
    columns:
      - name: budget_value
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0"
          - not_null
      - name: launch_status
        tests:
          - accepted_values:
              values: ['success', 'failure', 'partial']
          - not_null
      - name: launch_id
        tests:
          - unique
          - not_null
```

---

## Observabilidade

### Logger Estruturado JSON
```json
{
  "time": "2026-03-26T12:00:00",
  "level": "ERROR",
  "module": "spacex_full_pipeline",
  "message": "TASK FAIL: ingest_data",
  "dag_id": "spacex_full_pipeline",
  "run_id": "scheduled__2026-03-26T11:00:00+00:00"
}
```

### Alertas e SLA
- **Callback de falha:** Email automático via `ALERT_EMAIL` (`.env`)
- **SLA:** 30 minutos em tasks críticas
- **Retries:** Backoff exponencial (5 tentativas)
- **CI/CD:** Falhas em testes bloqueiam merge no GitHub

---

## Como Executar

### 1. Pré-requisitos
- Docker 20+
- Docker Compose 2+
- NASA API Key: [api.nasa.gov](https://api.nasa.gov)
- Python 3.9+ (para desenvolvimento local)

### 2. Configuração (.env)

```bash
# APIs
NASA_API_KEY=sua_chave_aqui
SPACEX_API_URL=https://api.spacexdata.com/v4/launches

# Database
POSTGRES_USER=admin
POSTGRES_PASSWORD=senha_segura
POSTGRES_DB=spacex_db

# Alertas
ALERT_EMAIL=seu_email@dominio.com
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/SEU/WEBHOOK/URL

# Airflow
AIRFLOW_UID=1000
```

### 3. Subir Infraestrutura

```bash
# Clone o repositório
git clone https://github.com/RicsonRamos/spacex_etl_pipeline.git
cd spacex_etl_pipeline

# Inicie os containers
docker compose up -d
```

Acesse os serviços:
- **Airflow:** http://localhost:8080 (login: `airflow` / `airflow`)
- **Metabase:** http://localhost:3000

### 4. Executar Testes Localmente

```bash
# Configurar PYTHONPATH
$env:PYTHONPATH = "."  # Windows
export PYTHONPATH=.    # Linux/Mac

# Rodar testes com cobertura
python -m pytest ingestion_engine/tests/ -v \
  --cov=src \
  --cov=config \
  --cov=main \
  --cov=ingestion_engine/utils \
  --cov-report=term-missing \
  --cov-fail-under=80
```

### 5. Executar Pipeline

1. Acesse Airflow: http://localhost:8080
2. Ative a DAG: `spacex_full_pipeline`
3. Trigger manual ou aguarde schedule diário

### 6. Acessar Dashboards

1. Acesse Metabase: http://localhost:3000
2. Configure conexão:
   - Host: `spacex_postgres`
   - Porta: `5432`
   - Database: `spacex_db`
   - User: `admin`
   - Password: `senha_segura`

---

## Estrutura de Pastas

```
spacex_etl_pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml                 # CI/CD GitHub Actions
├── config/
│   └── endpoints.py               # Configurações de endpoints
├── dags/
│   └── spacex_full_pipeline.py    # DAG principal do Airflow
├── dbt_spacex/
│   ├── models/
│   │   ├── bronze/                # Dados crus
│   │   ├── silver/                # Staging limpo
│   │   └── gold/                  # KPIs e marts
│   ├── tests/                     # Testes customizados
│   ├── schema.yml                 # Definições e constraints
│   └── packages.yml               # Dependências dbt
├── ingestion_engine/
│   ├── tests/                     # Testes unitários (58 testes)
│   │   ├── test_api_client.py
│   │   ├── test_api_extractor.py
│   │   ├── test_ingestion.py
│   │   ├── test_ingestion_quality.py
│   │   └── test_main.py
│   └── utils/
│       └── api_client.py          # Client SpaceX/NASA
├── src/
│   ├── extractors/                # Lógica de extração
│   ├── interfaces/                # Contratos/interfaces
│   ├── loaders/                   # Carregamento PostgreSQL
│   ├── models/                    # Schemas e validações
│   └── utils/                     # Logger e notificações
├── main.py                        # Motor de ingestão (orquestrador)
├── docker-compose.yml             # Orquestração containers
├── Dockerfile.airflow             # Imagem customizada
├── .env.example                   # Template de variáveis
└── README.md                      # Este arquivo
```

---

## Roadmap

- [x] **CI/CD GitHub Actions:** Testes automatizados com 82% de cobertura ✅
- [x] **Testes Unitários:** 58 testes com pytest e mocks ✅
- [ ] **Modelos Incrementais:** Otimização de I/O no dbt
- [ ] **dbt Docs:** Publicação automática via servidor estático
- [ ] **dbt-expectations:** Testes estatísticos avançados
- [ ] **Data Quality Dashboard:** Visão unificada de métricas de qualidade
- [ ] **Terraform:** Infraestrutura como código (AWS/GCP)

---

## Contribuição

Contribuições são bem-vindas! O projeto possui CI/CD robusto, então todas as PRs são validadas automaticamente.

1. Fork o projeto
2. Crie sua branch (`git checkout -b feature/nova-feature`)
3. **Certifique-se de que os testes passam:** `python -m pytest ingestion_engine/tests/ -v`
4. Commit suas mudanças (`git commit -m 'Adiciona nova feature'`)
5. Push para a branch (`git push origin feature/nova-feature`)
6. Abra um Pull Request (o CI vai validar automaticamente!)

---

## Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para detalhes.

---

## Autor

**Ricson Ramos**
- GitHub: [@RicsonRamos](https://github.com/RicsonRamos)
- LinkedIn: [linkedin.com/in/ricsonramos](https://linkedin.com/in/ricsonramos)

> ⭐ **Star** este repositório se ele te ajudou!

