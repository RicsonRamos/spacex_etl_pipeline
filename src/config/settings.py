from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from dotenv import load_dotenv
from typing import Optional
import os

# Carrega variáveis de ambiente do arquivo .env, se presente
load_dotenv()

class Settings(BaseSettings):
    """
    Configuração para a aplicação.
    Carrega configurações de variáveis de ambiente ou de um arquivo .env.
    """

    # Configuração da API do SpaceX
    SPACEX_API_URL: str = Field(
        default_factory=lambda: os.getenv("SPACEX_API_URL", "https://api.spacexdata.com/v4"), 
        description="URL base da API SpaceX"
    )
    RETRIES: int = Field(
        default_factory=lambda: int(os.getenv("RETRIES", 3)), 
        description="Número de tentativas para uma requisição falhada"
    )
    TIMEOUT: int = Field(
        default_factory=lambda: int(os.getenv("TIMEOUT", 10)), 
        description="Timeout em segundos para requisições da API"
    )

    # Configuração do banco de dados PostgreSQL
    POSTGRES_USER: str = Field(
        default_factory=lambda: os.getenv("POSTGRES_USER", "postgres"), 
        description="Usuário do banco de dados PostgreSQL"
    )
    POSTGRES_PASSWORD: str = Field(
        default_factory=lambda: os.getenv("POSTGRES_PASSWORD", "admin"), 
        description="Senha do banco de dados PostgreSQL"
    )
    POSTGRES_HOST: str = Field(
        default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost"), 
        description="Host ou IP do servidor PostgreSQL"
    )
    POSTGRES_PORT: int = Field(
        default_factory=lambda: int(os.getenv("POSTGRES_PORT", 5432)), 
        description="Porta do servidor PostgreSQL"
    )
    POSTGRES_DB: str = Field(
        default_factory=lambda: os.getenv("POSTGRES_DB", "spacex_db"), 
        description="Nome do banco de dados PostgreSQL"
    )

    # Variável para gerar a URL do banco de dados (dinâmica)
    @property
    def DATABASE_URL(self) -> str:
        """
        Constrói a URL de conexão com o PostgreSQL utilizando as configurações fornecidas.
        """
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    # Configuração opcional de alerta para Slack
    SLACK_WEBHOOK_URL: Optional[str] = Field(
        default_factory=lambda: os.getenv("SLACK_WEBHOOK_URL", None),
        description="URL do webhook do Slack para enviar alertas"
    )
    
    # Nova variável PREFECT_API_KEY
    PREFECT_API_KEY: Optional[str] = Field(
        default_factory=lambda: os.getenv("PREFECT_API_KEY", None),
        description="API Key do Prefect"
    )

    model_config = SettingsConfigDict(
        env_file=".env",  # Carrega as variáveis do arquivo .env
        extra="ignore",  # Ignora variáveis extras não definidas no modelo
        env_ignore_empty=True  # Ignora variáveis de ambiente vazias
    )

# Carrega a configuração do arquivo .env ou das variáveis de ambiente
settings = Settings()
