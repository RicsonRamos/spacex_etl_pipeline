import os
import yaml
from pathlib import Path
from dotenv import load_dotenv

# RIGOR: BASE_DIR deve apontar para a RAIZ do projeto (onde o main.py reside)
# Se este arquivo está em src/config/config.py, precisamos subir 2 níveis para chegar na raiz.
BASE_DIR = Path(__file__).resolve().parent.parent.parent

class Config:
    def __init__(self):
        load_dotenv(BASE_DIR / ".env", override=True)

        self.ROOT_DIR = BASE_DIR
        
        # CORREÇÃO: Verifique se o seu settings.yaml está na RAIZ/config/ ou em SRC/config/
        # O padrão de produção mais comum é na RAIZ do projeto.
        self.SETTING_PATH = self.ROOT_DIR / "config" / "settings.yaml"

        if not self.SETTING_PATH.exists():
            # Fallback para src/config caso você tenha movido para lá
            self.SETTING_PATH = self.ROOT_DIR / "src" / "config" / "settings.yaml"
            if not self.SETTING_PATH.exists():
                raise FileNotFoundError(f"Configuração não encontrada em: {self.ROOT_DIR}/config/ ou {self.ROOT_DIR}/src/config/")

        with self.SETTING_PATH.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}

        # --- API ---
        api_cfg = config.get("api", {})
        self.SPACEX_API_URL = api_cfg.get("base_url")
        self.API_ENDPOINTS = api_cfg.get("endpoints", {})

        # --- WHITELIST (Núcleo Duro) ---
        self.WHITELIST = config.get("whitelist", {})

        # --- DATABASE ---
        db_cfg = config.get("database", {})
        self.DATABASE_URL = db_cfg.get("url", f"sqlite:///{self.ROOT_DIR}/data/database/spacex_prod.db")
        
        # --- PIPELINE ---
        pipe_cfg = config.get("pipeline", {})
        self.TIMEOUT = int(pipe_cfg.get("timeout", 15))
        self.RETRIES = int(pipe_cfg.get("retries", 3))

settings = Config()