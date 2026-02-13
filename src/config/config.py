import os
from pathlib import Path
from dotenv import load_dotenv
import json

# CORREÇÃO: Sobe 3 níveis para garantir que BASE_DIR seja a raiz do projeto
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Carrega o arquivo .env da raiz
load_dotenv(BASE_DIR / ".env")

class Config:
    """Centraliza todas as configurações do pipeline."""
    
    # --- PATHS (Definidos primeiro para serem usados abaixo) ---
    # Usamos BASE_DIR.resolve() para garantir caminhos absolutos reais
    ROOT_DIR = BASE_DIR.resolve()
    RAW_DATA_DIR = ROOT_DIR / "data" / "raw"
    LOG_DIR = ROOT_DIR / "data" / "logs"
    
    # --- API CONFIGS ---
    SPACEX_API_URL = os.getenv("SPACEX_API_URL", "https://api.spacexdata.com/v4")
    
    # Tratamento de erro robusto para o Manifesto
    MANIFEST_PATH = ROOT_DIR / "manifesto.json"
    try:
        with open(MANIFEST_PATH, "r") as f:
            API_ENDPOINTS = json.load(f)["endpoints"]
    except (FileNotFoundError, KeyError) as e:
        # Fallback de segurança para o pipeline não "capotar" no início
        API_ENDPOINTS = {"rockets": "/rockets", "launches": "/launches"}
        print(f"⚠️ Aviso: Falha ao ler manifesto em {MANIFEST_PATH}. Usando padrão. Erro: {e}")

    # --- DATABASE CONFIGS ---
    # Garantimos que o caminho do SQLite também seja absoluto para o SQLAlchemy
    DB_PATH = ROOT_DIR / "data" / "spacex.db"
    DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{DB_PATH}")

    # --- PIPELINE PARAMS ---
    TIMEOUT = int(os.getenv("PIPELINE_TIMEOUT", 15))
    RETRIES = int(os.getenv("PIPELINE_RETRIES", 3))
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Instanciar para uso global
settings = Config()