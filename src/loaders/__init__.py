# projeto-etl-solid/
# ├── .github/workflows/      # CI/CD: Testes automáticos a cada "git push"
# ├── data/                   # NUNCA versione esta pasta (adicione ao .gitignore)
# │   ├── raw/                # Dados brutos (imutáveis)
# │   ├── processed/          # Dados após limpeza inicial
# │   └── gold/               # Dados prontos para o modelo de ML/Dashboard
# ├── src/                    # Onde o código "mora"
# │   ├── interfaces/         # Contratos e Classes Abstratas (Onde definimos o ABC)
# │   ├── extractors/         # Implementações de coleta (API, CSV, SQL)
# │   ├── transformers/       # Lógica de negócio e limpeza (Pureza funcional)
# │   ├── loaders/            # Escrita no destino (S3, BigQuery, Postgres)
# │   └── models/             # Definição de Schemas/Contratos de dados (Pydantic/Dataclasses)
# ├── tests/                  # Testes unitários para cada componente da src/
# ├── config/                 # Arquivos .yaml ou .env (Parâmetros de ambiente)
# ├── .gitignore              # Lista de arquivos para o Git ignorar
# ├── dvc.yaml                # Versionamento de dados (DVC)
# ├── requirements.txt        # Dependências do projeto
# └── main.py                 # O ponto de entrada (Orquestrador)
