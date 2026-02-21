
# 🚀 SpaceX Medallion ETL Pipeline

-[! [Python] (https://img.shields.io/badge/python-3.11 -blue)](https://www.python.org/)

-[! [Prefect)(https://img.shields.io/badge/prefect-2.0 -orange)] (https://www.prefect.io/)

-[! [License) (https://img.shields.io/badge/license-MIT -green)] (LICENSE)

+! [Python] (https://img.shields.io/badge/python-3.11 -blue)

+! [Prefect)(https://img.shields.io/badge/prefect-3.6.17 -orange)

+! [Postgres](https://img.shields.io/badge/postgres-16 -blue)

6 +! [Dockerized] (https://img.shields.io/badge/docker -ready-brightgreen)

Pipeline de dados de nível empresarial estruturado sob a arquitetura **Medallion**, projetado para extrair, transformar e carregar dados da API da SpaceX com foco em performance, tipagem rigorosa e observabilidade.



## 🏗️ Arquitetura e Decisões Técnicas

| Componente | Tecnologia | Justificativa Analítica |
| :--- | :--- | :--- |
| **Engine de Dados** | **Polars** | Processamento multi-threaded em Rust; superior ao Pandas em eficiência de memória e velocidade. |
| **Orquestração** | **Prefect 3.0** | Gerenciamento de estado, retentativas e monitoramento em tempo real (Observabilidade). |
| **Modelagem** | **dbt (Postgres)** | Transformações SQL modulares com testes de integridade e linhagem automática. |
| **Validação** | **Pydantic V2** | Garantia de contrato de dados (Data Contracts) na entrada da API. |
| **Infraestrutura** | **Docker** | Isolamento completo e reprodutibilidade via multi-stage builds. |

---

## 📊 Estrutura de Camadas (Medallion)

### 1. Bronze (Raw)
- **Origem:** REST API SpaceX.
- **Processo:** Extração via `SpaceXExtractor` com validação de schema.
- **Armazenamento:** Tabelas Postgres com coluna `raw_data` (JSONB) para garantir a re-processabilidade.

### 2. Silver (Cleansed)
- **Processo:** Limpeza, normalização e deduplicação via `SpaceXTransformer` (Polars).
- **Lógica de Carga:** Operações de **Upsert** no `PostgresLoader` para garantir idempotência.

### 3. Gold (Curated)
- **Processo:** Modelagem analítica via **dbt**.
- **Resultado:** Tabelas `fct_launches` e `dim_rockets` prontas para consumo em BI (PowerBI/Metabase).



---

## 📈 KPIs e Métricas de Sucesso

### Engenharia (Data Reliability)
- **Pipeline Latency:** Tempo total de execução do Flow (Target: < 5 min).
- **Data Freshness:** Idade do dado mais recente na Gold em relação ao tempo real.
- **Build Speed:** Tempo de build Docker otimizado via `uv` e cache.

### Negócio (Insights)
- **Launch Success Rate:** Taxa de sucesso por modelo de foguete.
- **Cost Analysis:** Custo acumulado por missão e eficiência financeira da frota.

---

## 🚀 Como Rodar

### Configuração de Ambiente
1. Clone o repositório:
   ```bash
   git clone [https://github.com/seu-usuario/spacex-etl.git](https://github.com/seu-usuario/spacex-etl.git)

 * Configure as variáveis de ambiente:
   cp .env.example .env
# Adicione suas credenciais do Postgres e Prefect API

Execução via Docker
O projeto está totalmente conteinerizado. Para iniciar o banco de dados e o pipeline:
docker-compose up --build

Execução Manual
# Instalar dependências rápidas via uv
uv pip install -e .

# Rodar ETL Completo
python main.py

# Rodar com Carga Incremental
python main.py --incremental

🧪 Qualidade e Testes
A suíte de testes utiliza pytest e testcontainers para garantir que o código funcione em ambientes reais antes do deploy.
# Rodar todos os testes com cobertura
pytest --cov=src tests/

 * Unit Tests: Validação de lógica de transformação.
 * Integration Tests: Validação de conexão e Upsert no Postgres.
 * Schema Tests: dbt tests para unicidade e integridade referencial.


Desenvolvido por: [Ricson Ramos]
Status: Produção / Estável ✅

---