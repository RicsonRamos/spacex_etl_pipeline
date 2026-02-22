# ---------------------------
# Estágio 1: Builder
# ---------------------------
FROM python:3.12-slim AS builder

WORKDIR /app

# Instala dependências de compilação para drivers de DB
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Instala dbt explicitamente se não estiver no pyproject.toml
# Se estiver no pyproject.toml, o comando install . abaixo resolve.
COPY pyproject.toml README.md ./
COPY src/ src/

# Instala dependências no site-packages do sistema
RUN uv pip install --system --no-cache . dbt-core dbt-postgres

# ---------------------------
# Estágio 2: Runtime
# ---------------------------
FROM python:3.12-slim

WORKDIR /app

# git é necessário para 'dbt deps', libpq5 para o driver do Postgres
RUN apt-get update && apt-get install -y \
    libpq5 \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copia o ambiente Python completo
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Garante que o dbt esteja no PATH e configurado
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    DBT_PROFILES_DIR=/app/dbt \
    DBT_PROJECT_DIR=/app/dbt

# Copia o projeto inteiro (incluindo a pasta dbt/ e models/)
COPY . .

# Comando de entrada original (o loop while true está no docker-compose)
CMD ["python", "-m", "src.flows.etl_flow"]