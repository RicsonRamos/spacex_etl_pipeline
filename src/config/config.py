import os
from pathlib import Path
from dotenv import load_dotenv

# Localiza o diretório raiz do projeto (onde o .env reside)
BASE_DIR = Path(__file__).resolve().parent.parent

# Carrega o arquivo .env se ele existir
load_dotenv(BASE_DIR / ".env")

class Config:
    """Centraliza todas as configurações do pipeline."""
    
    # --- API CONFIGS ---
    SPACEX_API_URL = os.getenv("SPACEX_API_URL", "https://api.spacexdata.com/v4")
    
    # Endpoints mapeados como dicionário para o Extractor
    API_ENDPOINTS = {
        "rockets": "/rockets",
        "launches": "/launches"
    }

    # --- DATABASE CONFIGS ---
    # Se estiver rodando no Docker, o path padrão será /app/data/...
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///data/spacex.db")

    # --- PATHS ---
    # Caminhos absolutos garantem que o Docker não se perca
    RAW_DATA_DIR = BASE_DIR / "data" / "raw"
    LOG_DIR = BASE_DIR / "data" / "logs"

    # --- PIPELINE PARAMS ---
    TIMEOUT = int(os.getenv("PIPELINE_TIMEOUT", 15))
    RETRIES = int(os.getenv("PIPELINE_RETRIES", 3))
    
    # --- LOGGING ---
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Instanciar para uso global
settings = Config()