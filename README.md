# SpaceX Production ETL Pipeline

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.5%2B-red)](https://airflow.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.5.0-orange)](https://www.getdbt.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14%2B-blue)](https://postgresql.org)
[![Metabase](https://img.shields.io/badge/Metabase-Latest-green)](https://metabase.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue)](https://docker.com)

&gt; Pipeline de dados completo para ingestão, transformação e visualização de missões espaciais da SpaceX e NASA.

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
  - [Decisões de Design](#decisões-de-design)
    - [A. Ciclo de Vida do dbt em Contêineres](#a-ciclo-de-vida-do-dbt-em-contêineres)
    - [B. Evolução dos Comandos (Entrypoint vs Command)](#b-evolução-dos-comandos-entrypoint-vs-command)
    - [C. Governança com Surrogate Keys](#c-governança-com-surrogate-keys)
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
    - [4. Executar Pipeline](#4-executar-pipeline)
    - [5. Acessar Dashboards](#5-acessar-dashboards)
  - [Estrutura de Pastas](#estrutura-de-pastas)
  - [Roadmap](#roadmap)
  - [Contribuição](#contribuição)
  - [Licença](#licença)
  - [Autor](#autor)

---

## Visão Geral

Este projeto implementa um pipeline de dados robusto que transforma dados brutos de APIs espaciais em inteligência de negócio acionável. A arquitetura segue o padrão **Medallion (Bronze, Silver, Gold)**, garantindo qualidade, rastreabilidade e governança em cada camada.

**Objetivos principais:**
- Automatizar a ingestão de dados da SpaceX e NASA
- Garantir qualidade através de testes rigorosos com dbt
- Disponibilizar dashboards em tempo real via Metabase
- Implementar observabilidade completa com logs estruturados e alertas

---

## Arquitetura

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
```

**Fluxo de Dados:**
1. **Bronze** → Ingestão raw via Python (APIs SpaceX/NASA)
2. **Silver** → Limpeza, padronização e surrogate keys (dbt)
3. **Gold** → Agregações e KPIs prontos para BI (dbt)

---

## Stack Tecnológico

| Componente | Tecnologia | Versão | Propósito |
|--------------|-----------|---------|-----------|
| **Orquestração** | Apache Airflow | 2.5+ | Agendamento e monitoramento |
| **Transformação** | dbt (data build tool) | 1.5.0 | Modelagem SQL + testes |
| **Warehouse** | PostgreSQL | 14+ | Armazenamento estruturado |
| **Visualização** | Metabase | Latest | Dashboards self-service |
| **Infraestrutura** | Docker + Compose | 20+ | Containerização total |
| **Ingestão** | Python + Requests | 3.9+ | ETL customizado |

---

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

## Decisões de Design

### A. Ciclo de Vida do dbt em Contêineres
**Desafio:** Sincronizar código dbt no Host (Windows/VS Code) com Airflow sem perder dependências.

**Solução:** Bind Mounts + task `dbt_deps` obrigatória na DAG.

**Benefício:** Garante que `packages.yml` seja resolvido antes de qualquer execução.

### B. Evolução dos Comandos (Entrypoint vs Command)
**Desafio:** Conflitos de redundância gerando `StatusCode 2` (`dbt dbt run`).

**Solução:** Refatoração da DAG para respeitar ENTRYPOINT da imagem Docker customizada.

### C. Governança com Surrogate Keys
**Desafio:** IDs de APIs externas podem ser instáveis ou duplicados entre fontes.

**Solução:** Surrogate Keys na camada Silver usando `dbt_utils.generate_surrogate_key`.

**Garantia:** Integridade referencial absoluta independente da fonte original.

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

### Exemplo de Testes (`schema.yml`)

```yaml
models:
  - name: fct_launches
    columns:
      - name: budget_value
        tests:
          - dbt_utils.expression_is_true:
              expression: "&gt;= 0"
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

---

## Como Executar

### 1. Pré-requisitos
- Docker 20+
- Docker Compose 2+
- NASA API Key: [api.nasa.gov](https://api.nasa.gov)

### 2. Configuração (.env)

```bash
# APIs
NASA_API_KEY=sua_chave_aqui

# Database
POSTGRES_USER=admin
POSTGRES_PASSWORD=senha_segura
POSTGRES_DB=spacex_db

# Alertas
ALERT_EMAIL=seu_email@dominio.com

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

### 4. Executar Pipeline

1. Acesse Airflow: [http://localhost:8080](http://localhost:8080)
2. Login: `airflow` / `airflow`
3. Ative a DAG: `spacex_full_pipeline`
4. Trigger manual ou aguarde schedule

### 5. Acessar Dashboards

1. Acesse Metabase: [http://localhost:3000](http://localhost:3000)
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
├── dags/
│   └── spacex_full_pipeline.py    # DAG principal do Airflow
├── dbt/
│   ├── models/
│   │   ├── bronze/                # Dados crus
│   │   ├── silver/                # Staging limpo
│   │   └── gold/                  # KPIs e marts
│   ├── tests/                     # Testes customizados
│   ├── schema.yml                 # Definições e constraints
│   └── packages.yml               # Dependências dbt
├── ingestion_engine/
│   ├── main.py                    # Script de ingestão
│   └── utils/
│       └── api_client.py          # Client SpaceX/NASA
├── docker-compose.yml             # Orquestração containers
├── Dockerfile.airflow             # Imagem customizada
├── .env.example                   # Template de variáveis
└── README.md                      # Este arquivo
```

---

## Roadmap

- [ ] **Modelos Incrementais:** Otimização de I/O no dbt
- [ ] **dbt Docs:** Publicação automática via servidor estático
- [ ] **dbt-expectations:** Testes estatísticos avançados
- [ ] **Data Quality Dashboard:** Visão unificada de métricas de qualidade
- [ ] **CI/CD:** GitHub Actions para testes e deploy
- [ ] **Terraform:** Infraestrutura como código (AWS/GCP)

---

## Contribuição

Contribuições são bem-vindas! Por favor:

1. Fork o projeto
2. Crie sua branch (`git checkout -b feature/nova-feature`)
3. Commit suas mudanças (`git commit -m 'Adiciona nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Abra um Pull Request

---

## Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para detalhes.

---

## Autor

**Ricson Ramos**
- GitHub: [@RicsonRamos](https://github.com/RicsonRamos)
- LinkedIn: [linkedin.com/in/ricsonramos](https://linkedin.com/in/ricsonramos)

---

&gt; ⭐ **Star** este repositório se ele te ajudou!