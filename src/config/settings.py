import os
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional
from pydantic_settings import SettingsConfigDict

class Settings(BaseSettings):
    """Gestão de configuração centralizada e tipada, pronta para ETL, logging e monitoramento."""

    # API SpaceX
    SPACEX_API_URL: str = "https://api.spacexdata.com/v4"
    API_RETRIES: int = Field(default=3, ge=1)
    API_TIMEOUT: int = Field(default=30, ge=1)

    # Banco de Dados
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str = Field(default_factory=lambda: os.getenv('POSTGRES_HOST_INTERNAL') if os.getenv('INSIDE_DOCKER') == '1' else os.getenv('POSTGRES_HOST_EXTERNAL'))
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str

    # Logging
    LOG_LEVEL: str = "INFO"  # compatível com tests/test_logging.py

    # Monitoramento / Alertas
    SLACK_WEBHOOK_URL: Optional[str] = None
    PROMETHEUS_PORT: int = 8000  # Porta padrão para métricas

    # ETL / Batch
    BATCH_SIZE: int = 1000

    @property
    def DATABASE_URL(self) -> str:
        """
        URL de conexão formatada para SQLAlchemy 2.0.
        Utiliza o driver Psycopg 3 (moderno/assíncrono).
        """
        # Garantir que as variáveis de ambiente essenciais estão configuradas
        if not self.POSTGRES_USER or not self.POSTGRES_PASSWORD or not self.POSTGRES_HOST or not self.POSTGRES_DB:
            raise ValueError("As variáveis de ambiente do banco de dados não estão configuradas corretamente.")
        
        # Formatar a URL de conexão do banco
        return (
            f"postgresql+psycopg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    # Configurações do Pydantic Settings
    model_config = SettingsConfigDict(
        env_file=".env",          # Carrega o arquivo .env
        env_file_encoding="utf-8", # Encoding do arquivo .env
        extra="ignore"            # Ignora variáveis extras no arquivo .env
    )

def get_settings() -> Settings:
    """
    Retorna uma instância do Settings.
    Use este getter em todo o projeto para manter consistência.
    """
    settings = Settings()
    print("Configurações carregadas:", settings.dict())  # Para depuração, imprime as variáveis
    return settings