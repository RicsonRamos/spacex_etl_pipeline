FROM python:3.11-slim

# PYTHONUNBUFFERED: Garante que os logs apareçam em tempo real
# PYTHONDONTWRITEBYTECODE: Evita que o container fique sujo com arquivos .pyc
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Instala dependências de compilação (necessário para SQLAlchemy/Pandas em imagens slim)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Prepara a estrutura de pastas antes de copiar o código
RUN mkdir -p data/raw data/logs

# Copia apenas os requisitos para aproveitar o cache de camadas do Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do projeto
COPY . .

# Comando de execução
CMD ["python", "main.py"]