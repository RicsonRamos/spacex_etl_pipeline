import yaml
from pathlib import Path
from dotenv import load_dotenv

# Get the root directory of the project (three levels up from this file)
BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Config:
    """Configuration manager for SpaceX ETL Pipeline."""

    # Default configuration paths in order of priority
    DEFAULT_SETTING_PATHS = [
        BASE_DIR / "config" / "settings.yaml",
        BASE_DIR / "src" / "config" / "settings.yaml",
    ]

    # Default values
    DEFAULTS = {
        "timeout": 15,
        "retries": 3,
        "database_url": "sqlite:///{}/data/database/spacex_prod.db",
    }

    def __init__(self, settings_path: Path = None):
        """
        Initialize configuration.

        :param settings_path: Optional explicit path to settings.yaml
        :raises FileNotFoundError: If configuration file not found
        :raises ValueError: If configuration is invalid
        """
        load_dotenv(BASE_DIR / ".env", override=True)

        self.ROOT_DIR = BASE_DIR
        self.SETTING_PATH = self._find_settings_file(settings_path)
        
        config = self._load_yaml_config(self.SETTING_PATH)
        
        self._load_api_config(config)
        self._load_database_config(config)
        self._load_pipeline_config(config)
        self._load_whitelist_config(config)

    def _find_settings_file(self, explicit_path: Path = None) -> Path:
        """Find settings file with validation."""
        if explicit_path:
            if not explicit_path.exists():
                raise FileNotFoundError(f"Settings file not found: {explicit_path}")
            return explicit_path

        for path in self.DEFAULT_SETTING_PATHS:
            if path.exists():
                return path

        raise FileNotFoundError(
            f"Configuration not found in: {', '.join(map(str, self.DEFAULT_SETTING_PATHS))}"
        )

    def _load_yaml_config(self, path: Path) -> dict:
        """Load and validate YAML configuration."""
        try:
            with path.open("r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
                return config if isinstance(config, dict) else {}
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in {path}: {e}")
        except IOError as e:
            raise FileNotFoundError(f"Cannot read {path}: {e}")

    def _load_api_config(self, config: dict) -> None:
        """Load API configuration."""
        api_cfg = config.get("api", {})
        self.SPACEX_API_URL = api_cfg.get("base_url")
        
        if not self.SPACEX_API_URL:
            raise ValueError("API base_url not configured")
        
        self.API_ENDPOINTS = api_cfg.get("endpoints", {})

    def _load_database_config(self, config: dict) -> None:
        """Load database configuration."""
        db_cfg = config.get("database", {})
        url = db_cfg.get("url")
        
        if not url:
            url = self.DEFAULTS["database_url"].format(self.ROOT_DIR)
        
        self.DATABASE_URL = url

    def _load_pipeline_config(self, config: dict) -> None:
        """Load pipeline configuration."""
        pipe_cfg = config.get("pipeline", {})
        
        try:
            self.TIMEOUT = int(pipe_cfg.get("timeout", self.DEFAULTS["timeout"]))
            self.RETRIES = int(pipe_cfg.get("retries", self.DEFAULTS["retries"]))
        except ValueError as e:
            raise ValueError(f"Invalid pipeline configuration: {e}")

    def _load_whitelist_config(self, config: dict) -> None:
        """Load whitelist configuration."""
        self.WHITELIST = config.get("whitelist", {})


# Create global settings instance
settings = Config()
