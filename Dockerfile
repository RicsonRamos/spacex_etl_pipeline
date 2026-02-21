# Estágio 1: Builder (Compilação de dependências)
FROM python:3.12-slim AS builder

WORKDIR /app

# Instala ferramentas de compilação necessárias
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Instala o 'uv'
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Instala dependências no ambiente do sistema (dentro do container)
COPY pyproject.toml .
# Usamos --no-cache para garantir que a imagem final seja limpa
RUN uv pip install --system --no-cache .

# Estágio 2: Runtime (Imagem final leve)
FROM python:3.12-slim

WORKDIR /app

# Dependência de runtime do Postgres (libpq é necessária para o psycopg2/psycopg)
RUN apt-get update && apt-get install -y \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copia as bibliotecas instaladas do estágio builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Variáveis de ambiente
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW=ignore

# Copia o código da aplicação
COPY . .

# Se houver packages.yml, o dbt precisa baixar as dependências agora
# RUN dbt deps --project-dir dbt/

# Comando de entrada usando a chamada de módulo correta para evitar ImportError
CMD ["python", "-m", "src.flows.etl_flow"]
