FROM python:3.11-slim

# PYTHONUNBUFFERED: Ensures logs appear in real-time
# PYTHONDONTWRITEBYTECODE: Prevents container from getting cluttered with .pyc files
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Installs build dependencies (necessary for SQLAlchemy/Pandas on slim images)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Prepares folder structure before copying code
RUN mkdir -p data/raw data/logs

# Copies only requirements to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copies the rest of the project
COPY . .

# Execution command
CMD ["python", "main.py"]