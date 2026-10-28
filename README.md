# SpaceX Production ETL Pipeline

End-to-end data engineering platform orchestrating the extraction, transformation, modeling, and visualization of aerospace missions from the SpaceX public API and NASA datasets.

---

## 1. Problema de Negócio e Contexto

No setor aeroespacial comercial, a eficiência operacional e o retorno sobre investimento dependem diretamente da capacidade de reuso de propulsores (*boosters*), da redução do custo por quilograma lançado em órbita e da confiabilidade das janelas de lançamento.

Entretanto, as informações operacionais e de telemetria de missões encontram-se frequentemente dispersas em endpoints REST sem padronização relacional, com esquemas dinâmicos, tipos aninhados e ausência de garantias de integridade histórica.

O objetivo deste projeto foi conceber e implantar uma plataforma de dados de nível produtivo capaz de:
- Consumir dados transacionais de missões da SpaceX via API REST com resiliência a falhas de rede.
- Organizar e transformar o fluxo de dados em um Data Warehouse estruturado segundo a Arquitetura Medalhão (Bronze, Silver, Gold).
- Modelar indicadores de missão e taxas de reutilização de estágios de foguetes para consumo em Business Intelligence.
- Orquestrar todo o ciclo de vida analítico de forma autônoma e conteinerizada.

---

## 2. Processo e Racional das Escolhas Técnicas

### 2.1. Orquestração com Apache Airflow vs. Agendadores Simples (Cron / Scripts Ad-hoc)
- **Racional:** Pipelines corporativos exigem rastreabilidade de DAGs, idempotência, tratamento determinístico de retries e observabilidade de SLAs. O Apache Airflow foi selecionado para orquestrar as etapas de ingestão em Python, execução de dependências de pacotes dbt e build de modelos relacionais em ordem topológica estrita.
- **Isolamento de Contêineres:** Foi implementada uma imagem customizada para o worker do Airflow e um contêiner dedicado para execução dos comandos do dbt via bind mounts, assegurando que o `packages.yml` seja resolvido (`dbt deps`) sem poluir o ambiente de execução dos operadores base.

### 2.2. Arquitetura Medalhão e Transformação com dbt
- **Camada Bronze (Raw Ingestion):** Armazena o payload bruto extraído da API no PostgreSQL, preservando a imutabilidade do dado transacional para propósitos de auditoria e reprocessamento (*replayability*).
- **Camada Silver (Staging & Cleansing):** Desaninhamento de estruturas JSON complexas (cargas úteis, núcleos de propulsão e telemetria de pouso), conversão rigorosa de tipos de dados e geração de Surrogate Keys (SK).
- **Surrogate Keys vs. Natural Keys:** Chaves naturais provenientes de APIs externas sofrem risco de descontinuidade, duplicação ou mutação de esquema. A geração de Surrogate Keys via `dbt_utils.generate_surrogate_key` garantiu unicidade referencial e integridade entre dimensões e fatos.
- **Camada Gold (Analytics Marts):** Modelagem dimensional orientada ao negócio (ex.: fatos de missões, dimensões de plataformas de lançamento e propulsores), otimizada para consultas analíticas agregadas no Metabase.

### 2.3. Resolução de Falhas Arquiteturais Documentadas
- **Controle de Entrypoint no dbt:** Durante o desenvolvimento, identificou-se e corrigiu-se um conflito de redundância de comando (`StatusCode 2`: chamada duplicada `dbt dbt run`) através da harmonização entre o `ENTRYPOINT` do contêiner Docker e os argumentos passados pelo operador do Airflow.
- **Failsafe e Qualidade:** Configuração de testes automáticos do dbt (`unique`, `not_null`, `relationships` e `accepted_values`) bloqueando a promoção de dados corrompidos para a camada Gold.

---

## 3. Resultados Atingidos e Métricas de Validação

### 3.1. KPIs de Engenharia e Operacionais
- **Autonomia Operacional:** Pipeline 100% conteinerizado executando ingestão programada com tempo de processamento inferior a 3 minutos para a carga completa.
- **Integridade de Dados:** 100% de conformidade nos testes de integridade referencial nas tabelas de staging e marts, eliminando chaves órfãs entre missões e núcleos de propulsão.

### 3.2. Métricas de Negócio Extraídas (Marts Aeroespaciais)
- **Taxa de Reutilização de Propulsores:** Identificação quantitativa da evolução da taxa de sucesso de pouso e reuso de boosters Falcon 9 ao longo dos anos.
- **Custo e Carga Útil:** Agregação de massa transportada (kg) por órbita de destino (LEO, GTO, Polar) e taxa de sucesso por base de lançamento (KSC LC-39A, CCAFS SLC-40, VAFB SLC-4E).

---

## 4. Arquitetura da Solução e Estrutura

```text
spacex_etl_pipeline/
├── dags/
│   └── spacex_etl_dag.py        # DAG do Airflow orquestrando ingestão e dbt
├── dbt_spacex/                  # Projeto analítico dbt
│   ├── dbt_project.yml          # Configurações do projeto dbt
│   ├── packages.yml             # Dependências (dbt_utils)
│   ├── profiles.yml             # Conexão com o PostgreSQL
│   ├── models/                  # Camadas Medallion
│   │   ├── staging/             # Camada Silver (limpeza, tipos e surrogate keys)
│   │   └── marts/               # Camada Gold (dimensões e fatos analíticos)
│   └── tests/                   # Testes customizados de integridade
├── ingestion_engine/            # Módulos Python de extração da API SpaceX
├── analytics/                   # Consultas SQL e relatórios consolidados
├── docker-compose.yml           # Orquestração do ecossistema (Airflow, Postgres, Metabase)
├── Dockerfile.airflow           # Imagem customizada do Apache Airflow
├── Dockerfile.dbt               # Imagem customizada do dbt Core
└── metabase_setup.py            # Automação de provisionamento de dashboards
```

---

## 5. Como Executar

### 5.1. Pré-requisitos
- Docker Engine 20.10+
- Docker Compose v2+
- Mínimo de 4 GB de memória RAM livre para o cluster de contêineres

### 5.2. Inicialização dos Serviços

1. Clone o repositório:
```bash
git clone https://github.com/RicsonRamos/spacex_etl_pipeline.git
cd spacex_etl_pipeline
```

2. Crie o arquivo de variáveis de ambiente com base no exemplo:
```bash
cp .env.example .env
```

3. Inicialize os bancos de dados e serviços:
```bash
docker compose up -d postgres airflow-init
```

4. Suba a stack completa (Airflow Webserver/Scheduler, Postgres, Metabase):
```bash
docker compose up -d
```

### 5.3. Acessos e Validação

- **Airflow UI:** `http://localhost:8080` (usuário padrão: `airflow` / senha: `airflow`). Ative a DAG `spacex_etl_pipeline`.
- **Metabase BI:** `http://localhost:3000`. Conecte ao banco de dados `spacex_dw` para visualizar os painéis analíticos.
- **Execução manual de testes dbt:**
```bash
docker compose run --rm dbt dbt test
```
