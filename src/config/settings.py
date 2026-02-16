from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Gerenciamento centralizado de configurações usando Pydantic.
    Prioridade de carga: 
    1. Variáveis de ambiente (OS) 
    2. Arquivo .env 
    3. Valores padrão (default)
    """
    
    # --- API CONFIG ---
    SPACEX_API_URL: str = Field(default="https://api.spacexdata.com/v4")
    RETRIES: int = Field(default=3)
    TIMEOUT: int = Field(default=10)

    # --- POSTGRES CONFIG ---
    # Nota: O host default 'db' refere-se ao nome do serviço no docker-compose.
    POSTGRES_USER: str = Field(default="postgres")
    POSTGRES_PASSWORD: str = Field(default="admin")
    POSTGRES_HOST: str = Field(default="db") 
    POSTGRES_PORT: int = Field(default=5432)
    POSTGRES_DB: str = Field(default="spacex_db")

    # --- LOGGING ---
    LOG_LEVEL: str = Field(default="INFO")

    @computed_field
    @property
    def DATABASE_URL(self) -> str:
        """Monta a URL de conexão baseada nos componentes atuais."""
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    # --- MODEL CONFIG ---
    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding="utf-8",
        extra="ignore",
        # Permite que variáveis de ambiente em MAIÚSCULO preencham os campos
        case_sensitive=True 
    )

# Instância singleton para uso em todo o projeto
settings = Settings()