# 🚀 **SpaceX Medallion ETL Pipeline**

![Python](https://img.shields.io/badge/python-3.12-blue)
![Prefect](https://img.shields.io/badge/prefect-3.0-orange)
![Postgres](https://img.shields.io/badge/postgres-16-blue)
![Docker](https://img.shields.io/badge/docker-ready-brightgreen)
![Ruff](https://img.shields.io/badge/linter-ruff-000000)

Pipeline de dados de nível empresarial estruturado sob a arquitetura **Medallion**, projetado para extrair, transformar e carregar dados da API da SpaceX com foco em **performance**, **tipagem rigorosa** e **observabilidade**.

---

## 🏗️ **Arquitetura e Decisões Técnicas**

| **Componente**      | **Tecnologia**     | **Justificativa Analítica**                                                        |
| ------------------- | ------------------ | ---------------------------------------------------------------------------------- |
| **Engine de Dados** | **Polars**         | Processamento multi-threaded em Rust; superior ao Pandas em eficiência de memória. |
| **Orquestração**    | **Prefect 3.0**    | Gerenciamento de estado, retentativas automáticas e observabilidade nativa.        |
| **Modelagem**       | **dbt (Postgres)** | Transformações SQL modulares com testes de integridade e linhagem automática.      |
| **Validação**       | **Pydantic V2**    | Garantia de contrato de dados (Data Contracts) na entrada da API.                  |
| **Infraestrutura**  | **Docker**         | Isolamento completo e reprodutibilidade via multi-stage builds.                    |

---

## 📊 **Estrutura de Camadas (Medallion)**

### 1. **Bronze (Raw)**

* **Origem:** REST API SpaceX.
* **Processo:** Extração via `SpaceXExtractor` com validação de schema usando Pydantic.
* **Armazenamento:** Tabelas Postgres com coluna `raw_data` (JSONB) para garantir rastreabilidade total.

### 2. **Silver (Cleansed)**

* **Processo:** Limpeza, normalização e deduplicação via `SpaceXTransformer` (Polars).
* **Lógica de Carga:** Operações de **Upsert (Merge)** para garantir idempotência técnica e integridade.

### 3. **Gold (Curated)**

* **Processo:** Modelagem analítica via **dbt**.
* **Resultado:** Tabelas `fct_launches` e `dim_rockets` otimizadas para consumo em ferramentas de BI.

---

## 📈 **KPIs e Métricas de Sucesso**

### **Engenharia (Data Reliability)**

* **Pipeline Latency:** Tempo total de execução do Flow (Target: < 5 min).
* **Data Freshness:** Idade do dado na camada Gold em relação ao evento real na API.
* **Build Speed:** Tempo de build Docker otimizado via `uv` e cache de camadas.

### **Negócio (Insights)**

* **Launch Success Rate:** Taxa de sucesso por modelo de foguete e local de lançamento.
* **Cost Analysis:** Custo acumulado por missão e eficiência financeira da frota.

---

## 🚀 **Como Rodar**

### **Configuração de Ambiente**

1. Clone o repositório:

   ```bash
   git clone https://github.com/seu-usuario/spacex-etl.git
   cd spacex-etl
   ```

2. Configure as variáveis de ambiente:

   ```bash
   cp .env.example .env
   # Edite o .env com suas credenciais do Postgres e Prefect API
   ```

### **Execução via Docker**

O projeto está totalmente conteinerizado para garantir paridade entre ambientes:

```bash
docker-compose up --build
```

### **Execução Manual**

1. Instalar dependências ultrarrápidas via `uv`:

   ```bash
   uv pip install -e .
   ```

2. Rodar ETL Completo:

   ```bash
   python main.py
   ```

3. Rodar com Carga Incremental:

   ```bash
   python main.py --incremental
   ```

---

## 🧪 **Qualidade e Testes**

A suíte de testes utiliza **pytest** e **testcontainers** para validar o pipeline em condições reais de banco de dados.

1. Rodar todos os testes com relatório de cobertura:

   ```bash
   pytest --cov=src tests/ -v
   ```

2. Tipos de Testes:

   * **Unit Tests:** Validação de lógica de transformação e contratos Pydantic.
   * **Integration Tests:** Validação de persistência e Upsert no Postgres usando containers efêmeros.
   * **Schema Tests:** Testes dbt para garantir unicidade e integridade referencial.

---

## 📅 **Desenvolvimento e Status**

* **Desenvolvido por:** Ricson Ramos
* **Status:** Produção / Estável ✅

---

### 📚 **Tecnologias Usadas**

* **Python 3.12** para desenvolvimento.
* **Prefect 3.0** para orquestração de fluxo.
* **PostgreSQL 16** como banco de dados relacional.
* **Docker** para garantir ambiente reprodutível e isolamento.
* **Polars** para processamento de dados com eficiência.
* **dbt** para modelagem analítica e integração de dados.

---

