FROM python:3.12-slim

# Instala dependências de sistema para psycopg2 e ferramentas de rede
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instala o 'uv' para gestão ultra-rápida de pacotes
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# Variáveis de ambiente para Python
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Instala dependências antes de copiar o código (aproveita cache do Docker)
COPY pyproject.toml .
RUN uv pip install --system .

# Copia o restante da aplicação
COPY . .

# Comando de entrada
CMD ["python", "src/main.py"]