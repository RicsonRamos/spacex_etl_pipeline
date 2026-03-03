# Rigor: Usar uma imagem slim para manter o container leve
FROM python:3.11-slim

# Instala dependências de sistema necessárias para o driver do Postgres (psycopg2)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copia e instala dependências Python primeiro para aproveitar o cache de camadas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do código
COPY . .

# Define o ponto de entrada do motor de ingestão
CMD ["python", "main.py"]