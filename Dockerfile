# Base image Python 3.12 slim
FROM python:3.12-slim

# Instala dependências de sistema para psycopg2 e ferramentas de rede
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instala o 'uv' (gestão de pacotes ultrarrápida)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Define diretório de trabalho
WORKDIR /app

# Variáveis de ambiente para Python
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Copia pyproject.toml antes do código para aproveitar cache Docker
COPY pyproject.toml .

# Instala dependências via uv
RUN uv pip install --system .
RUN pip install "dbt-postgres~=1.7"

# Copia todo o código da aplicação
COPY . .

# Supress warnings do Prefect fora de flows
ENV PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW=ignore

# Comando de entrada padrão
CMD ["python", "src/main.py"]