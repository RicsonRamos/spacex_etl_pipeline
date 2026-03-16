## SpaceX Production ETL Pipeline
Data Engineering & Analytics Architecture

Este projeto implementa um pipeline de dados robusto para ingestão, transformação e visualização de dados de missões espaciais da SpaceX e NASA. 
O objetivo é transformar dados brutos de APIs em inteligência de negócio através de uma arquitetura Medallion (Bronze, Silver, Gold).

### 1. Arquitetura e Stack Técnica
Oprojeto foi desenhado sob o princípio da segregação de responsabilidades e conteinerização total:

- Orquestração: Apache Airflow (Dockerized).
- Ingestão (Bronze): Ingestion Engine customizado em Python.
- Transformação (Silver/Gold): dbt (data build tool) 1.5.0.
- Banco de Dados: PostgreSQL (Warehouse).
- Visualização: Metabase (BI).
- Infraestrutura: Docker & Docker Compose.

### 2. Decisões de Design e Raciocínio Técnico

A. Ciclo de Vida do dbt em Contêineres

- Desafio: Sincronizar o código do dbt no Host (Windows/VS Code) com o ambiente de execução Airflow sem perder dependências.
- Solução: Implementação de Bind Mounts para desenvolvimento em tempo real, combinada com uma task de dbt_deps na DAG.
- Garantir que o packages.yml seja sempre resolvido antes da execução, evitando quebras por falta do dbt_utils.

B. Evolução dos Comandos (Entrypoint vs Command)

- Desafio: Conflitos de redundância gerando StatusCode 2 (Erro: dbt dbt run).
- Solução: Refatoração da DAG para remover o prefixo dbt dos comandos, respeitando o ENTRYPOINT da imagem customizada.

C. Governança com Surrogate Keys

- Desafio: IDs de APIs externas podem ser instáveis ou duplicados entre fontes.
- Solução: Implementação de Surrogate Keys na camada Silver usando MD5 hashes (dbt_utils.generate_surrogate_key).
- Garantir integridade referencial absoluta e unicidade, independentemente da fonte original.

### 3. Estrutura de Dados (Medallion)

1. Bronze (Raw): Dados crus da SpaceX/NASA ingeridos via API e persistidos no Postgres sem alterações de schema.
2. Silver (Staging): Limpeza de tipos, padronização de datas (UTC) e criação da launch_key.
3. Gold (Marts): Tabelas agregadas focadas em KPIs, prontas para o consumo do Metabase.

### 4. KPIs de Negócio (Camada Gold)

O pipeline foi otimizado para responder a:

- Taxa de Sucesso por Ano: Porcentagem de lançamentos bem-sucedidos.
- Eficiência de Custo: Cruzamento de dados orçamentários da NASA com o volume de carga útil da SpaceX.
- Densidade de Lançamentos: Frequência de missões por rocket_type e launch_pad.

### 5. Protocolo de Qualidade (Rigor)

O sistema interrompe o fluxo automaticamente (Failsafe) se:

- `dbt_test` falhar: Testes de `unique` e `not_null` na camada Staging garantem que dados "sujos" não cheguem ao dashboard.
- `source_freshness` falhar: Se a ingestão (Bronze) não trouxe dados novos, o dbt não processa a Gold para evitar relatórios defasados.

### 6. Como Executar

1. Variáveis de Ambiente: Configure seu .env com NASA_API_KEY e credenciais do Postgres.
2. Up Infra:
```bash
docker compose up -d
```

3. Executar Pipeline:

- Acesse o Airflow (`localhost:8080`).
- Trigger na DAG `spacex_full_pipeline`.

4. Acessar Dashboards:

- Acesse o Metabase (`localhost:3000`).
- Conecte ao banco `spacex_db` (host: spacex_postgres).
- 


### 7. Próximas Evoluções 

[ ] Implementação de Modelos Incrementais para otimização de custo de I/O.
[ ] Publicação automática do dbt Docs via servidor estático.
[ ] Expansão de testes com a biblioteca dbt_expectations.
