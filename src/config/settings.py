from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, computed_field
from typing import Optional

class Settings(BaseSettings):
    """Gestão de configuração centralizada e tipada."""

    # API SpaceX
    SPACEX_API_URL: str = "https://api.spacexdata.com/v4"
    API_RETRIES: int = Field(default=3, ge=1)
    API_TIMEOUT: int = Field(default=30, ge=1)

    # Database (Obrigatórios no .env)
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str

    # Monitoramento
    SLACK_WEBHOOK_URL: Optional[str] = None

    @computed_field
    @property
    def DATABASE_URL(self) -> str:
        """URL formatada para SQLAlchemy 2.0 / Psycopg 3."""
        return (
            f"postgresql+psycopg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"  # Ignora campos extras no .env sem travar
    )

def get_settings() -> Settings:
    return Settings()