FROM python:3.12-slim

# Instala dependências de sistema essenciais
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Instala o UV usando o instalador oficial (Garante que o binário funcione no ambiente slim)
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh
ENV PATH="/root/.local/bin/:$PATH"

WORKDIR /app

# Evita arquivos .pyc e permite logs em tempo real
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Copia os arquivos de definição de projeto
COPY pyproject.toml .
# Caso exista o lockfile, remova o comentário abaixo
# COPY uv.lock . 

# Instala as dependências diretamente no Python do sistema do container
RUN uv pip install --system .

# Copia o restante do código
COPY . .

CMD ["python", "-m", "src.main"]