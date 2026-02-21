# SpaceX ETL Pipeline

![Python](https://img.shields.io/badge/python-3.11-blue)
![Prefect](https://img.shields.io/badge/prefect-3.6.17-orange)
![Postgres](https://img.shields.io/badge/postgres-16-blue)
![Dockerized](https://img.shields.io/badge/docker-ready-brightgreen)

Este projeto implementa uma arquitetura Medallion (Bronze/Silver/Gold) para processamento de dados da SpaceX API, utilizando o estado da arte em Engenharia de Dados em 2026: Polars para processamento, Prefect 3.0 para orquestração e dbt para modelagem analítica.
🛠 Stack Técnica & Escolhas Arquiteturais
| Tecnologia | Escolha | Justificativa Técnica |
|---|---|---|
| Linguagem | Python 3.12+ | Aproveitamento de Type Hinting avançado e melhor performance do interpretador. |
| Engine | Polars | Superior ao Pandas em uso de memória (Zero-copy) e performance multi-threaded para transformações Silver. |
| Orquestrador | Prefect 3.0 | Observabilidade nativa, retentativas automáticas e desacoplamento total da infraestrutura. |
| Modelagem | dbt (Data Build Tool) | Garantia de linhagem de dados, testes automatizados de schema e documentação SQL-based. |
| Interface DB | SQLAlchemy 2.0 | Uso de mapeamento moderno e drivers assíncronos (psycopg3) para maior vazão de I/O. |
| Gestor de Pack | uv | Instalação de dependências até 10x mais rápida que o pip, garantindo CI/CD ágil. |
🏗 Arquitetura de Dados (Medallion)
 * Bronze (Raw): Ingestão via SpaceXExtractor. O dado é salvo em formato JSONB no Postgres para auditoria completa e re-processabilidade.
 * Silver (Cleaned): O SpaceXTransformer utiliza Polars para tipagem rigorosa, tratamento de nulos e normalização. O PostgresLoader realiza operações de Upsert (Merge) baseado em chaves primárias.
 * Gold (Curated): Modelos dbt transformam os dados em tabelas de fatos (fct_launches) e dimensões (dim_rockets), otimizadas para BI e Analytics.
📈 Métricas de Engenharia & KPIs de Negócio
Para garantir a saúde do pipeline e o valor para o negócio, monitoramos:
Métricas de Qualidade de Dados (Engenharia)
 * Freshness (SLA): Tempo entre o lançamento na API e a disponibilidade na camada Gold (Target: < 1 hora).
 * Data Completeness: % de registros na Gold em relação à Bronze (Target: 100%).
 * Schema Drift: Número de falhas de validação Pydantic no SpaceXExtractor.
KPIs de Negócio (Analytics)
 * Success Rate by Rocket: Taxa de sucesso por tipo de foguete (Dimensão vs Fato).
 * Cost Efficiency: Custo médio por kg colocado em órbita (Calculado na camada Gold).
 * Launch Frequency: Volume de lançamentos mensais para análise de capacidade da frota.
🚀 Como Executar
Pré-requisitos
 * Docker & Docker Compose
 * Prefect Cloud API Key (Opcional para execução local)
Instalação e Execução
 * Clone o repositório e configure o ambiente:
   cp .env.example .env
# Edite o .env com suas credenciais

 * Suba o ecossistema (Banco + ETL + Dashboard):
   docker-compose up --build

 * Execução Manual via CLI:
   # Carga completa
python main.py
# Carga incremental (apenas novos registros)
python main.py --incremental

🧪 Estratégia de Testes
 * Unitários (pytest): Validam a lógica de transformação do Polars isoladamente.
 * Integração (testcontainers): Sobe um banco efêmero para validar o Upsert do Loader.
 * Schema Tests (dbt): Validam unicidade e integridade referencial na camada Gold.
<!-- end list -->
# Executar suíte de testes completa
pytest tests/ --cov=src -v

🛡 Segurança e Boas Práticas
 * Zero Hardcode: Todas as credenciais são injetadas via variáveis de ambiente validadas pelo Pydantic Settings.
 * CI/CD: Pipeline no GitHub Actions que executa Linter (Ruff), Testes e Build da imagem Docker em cada Push.
 * Isolamento de Redes: O banco de dados Postgres não expõe portas para a internet, sendo acessível apenas pelo serviço de ETL.
Analista Responsável: Ricson Ramos
Status do Projeto: Produção / Estável
Diagnóstico de Impacto do README
Este documento posiciona o seu projeto como uma solução de missão crítica. Ele explica o porquê de cada ferramenta, o que é essencial para avaliações técnicas de alto nível.
Agora que a documentação está pronta, você gostaria que eu ajudasse a configurar o agendamento (Schedule) no Prefect para que esse pipeline rode automaticamente todos os dias às 00:00?
