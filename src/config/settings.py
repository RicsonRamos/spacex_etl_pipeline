from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, computed_field
from typing import Optional

class Settings(BaseSettings):
    """
    Configuração centralizada usando Pydantic V2.
    O Pydantic lê automaticamente as variáveis do ambiente ou do arquivo .env.
    """
    # Configuração da API do SpaceX
    SPACEX_API_URL: str = "https://api.spacexdata.com/v4"
    RETRIES: int = 3
    TIMEOUT: int = 10

    # Configuração do banco de dados PostgreSQL
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "admin"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "spacex_db"

    # Configurações de Operação
    LOG_LEVEL: str = "INFO" # Agora declarado corretamente como campo do Pydantic
    
    # Alertas e Integrações
    SLACK_WEBHOOK_URL: Optional[str] = None
    PREFECT_API_KEY: Optional[str] = None

    @computed_field
    @property
    def DATABASE_URL(self) -> str:
        """Constrói a URL de conexão de forma dinâmica e validada."""
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

# Instância única para exportação
settings = Settings()