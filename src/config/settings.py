from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import computed_field
from typing import Optional
import os

class Settings(BaseSettings):
    """
    Configuração centralizada usando Pydantic V2.
    Lê variáveis do ambiente ou do arquivo .env automaticamente.
    """

    # SpaceX API
    SPACEX_API_URL: str = "https://api.spacexdata.com/v4"
    RETRIES: int = 3
    TIMEOUT: int = 10

    # PostgreSQL Database
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "admin"
    POSTGRES_HOST_INTERNAL: str = "db"        # Dentro do Docker
    POSTGRES_HOST_EXTERNAL: str = "localhost" # Fora do Docker / CI
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "spacex_db"

    # Detecta se está dentro do Docker ou CI
    INSIDE_DOCKER: Optional[int] = 0  # 1 = dentro do container, 0 = fora

    @computed_field
    @property
    def POSTGRES_HOST(self) -> str:
        """
        Retorna o host correto de acordo com o ambiente.
        GitHub Actions e execução local usam localhost.
        Docker container usa o host interno.
        """
        # CI/CD detecta pelo ambiente do GitHub Actions
        if os.getenv("GITHUB_ACTIONS", "false").lower() == "true":
            return "localhost"
        return self.POSTGRES_HOST_INTERNAL if self.INSIDE_DOCKER else self.POSTGRES_HOST_EXTERNAL

    @computed_field
    @property
    def DATABASE_URL(self) -> str:
        """Constrói a URL de conexão PostgreSQL de forma dinâmica."""
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    # Logging
    LOG_LEVEL: str = "INFO"

    # Prefect Cloud
    PREFECT_API_KEY: Optional[str] = None
    PREFECT_API_URL: Optional[str] = None

    # Alertas
    SLACK_WEBHOOK_URL: Optional[str] = None

    # Configuração do Pydantic
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

# Instância global
settings = Settings()