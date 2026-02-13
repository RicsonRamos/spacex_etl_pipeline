import os
import json
from pathlib import Path
from dotenv import load_dotenv

# Absolute path to the project root
BASE_DIR = Path(__file__).resolve().parent.parent.parent

class Config:
    """
    Configuration manager for the SpaceX ETL Pipeline.
    Loads environment variables from .env and endpoint definitions from manifesto.json.
    """
    def __init__(self):
        """
        Initializes settings, ensures directory structure, and validates core parameters.
        """
        # Load .env file with override to ensure latest values are used
        load_dotenv(BASE_DIR / ".env", override=True)
        
        self.ROOT_DIR = BASE_DIR
        self.RAW_DATA_DIR = self.ROOT_DIR / "data" / "raw"
        self.LOG_DIR = self.ROOT_DIR / "data" / "logs"
        self.MANIFEST_PATH = self.ROOT_DIR / "manifesto.json"
        
        # Ensure infrastructure directories exist
        self.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.LOG_DIR.mkdir(parents=True, exist_ok=True)

        # Critical Configurations
        self.SPACEX_API_URL = self._get_env_var_or_raise("SPACEX_API_URL")
        self.API_ENDPOINTS = self._load_manifest()

        # Optional Configurations with Defaults
        self.DATABASE_URL = self._get_env_var_or_default(
            "DATABASE_URL", 
            f"sqlite:///{self.ROOT_DIR}/data/spacex_etl.db"
        )
        self.TIMEOUT = int(self._get_env_var_or_default("PIPELINE_TIMEOUT", 15))
        self.RETRIES = int(self._get_env_var_or_default("PIPELINE_RETRIES", 3))
        self.LOG_LEVEL = self._get_env_var_or_default("LOG_LEVEL", "INFO").upper()

    def _get_env_var_or_raise(self, var_name: str) -> str:
        """
        Retrieves an environment variable or raises ValueError if missing.
        """
        val = os.getenv(var_name)
        if not val:
            raise ValueError(f"CRITICAL: {var_name} is not defined in .env file!")
        return val  # FIXED: Added missing return

    def _get_env_var_or_default(self, var_name: str, default: any) -> any:
        """
        Retrieves an environment variable or returns a default value.
        """
        val = os.getenv(var_name)
        return val if val is not None else default

    def _load_manifest(self) -> dict:
        """
        Loads the manifesto.json file containing endpoint mappings.
        """
        if not self.MANIFEST_PATH.exists():
            raise FileNotFoundError(f"Manifest missing at: {self.MANIFEST_PATH}")
            
        try:
            with self.MANIFEST_PATH.open("r", encoding="utf-8") as f:
                manifest = json.load(f)
            return manifest["endpoints"]
        except (json.JSONDecodeError, KeyError) as e:
            raise RuntimeError(f"Fatal error parsing manifesto.json: {e}")

# Global settings instance
settings = Config()