from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, computed_field
from typing import Optional

class Settings(BaseSettings):
    """Gestão de configuração centralizada."""

    # API SpaceX
    SPACEX_API_URL: str = "https://api.spacexdata.com/v5"
    API_RETRIES: int = Field(default=3, ge=1)
    API_TIMEOUT: int = Field(default=30, ge=1) # Aumentado para 30s por segurança

    # Database
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str

    # Notificações (Utilizado pelo Flow/Monitoring)
    SLACK_WEBHOOK_URL: Optional[str] = None

        @computed_field
    @property
    def DATABASE_URL(self) -> str:
        """URL otimizada para Psycopg 3 (Driver moderno de 2026)."""
        return (
            f"postgresql+psycopg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()
