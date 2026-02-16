import os
from typing import Optional
from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # --- API CONFIG ---
    SPACEX_API_URL: str = Field(default="https://api.spacexdata.com/v4")
    RETRIES: int = Field(default=3)
    TIMEOUT: int = Field(default=10)

    # --- POSTGRES CONFIG ---
    # O Pydantic lerá das envs do Docker (POSTGRES_HOST=db) automaticamente
    POSTGRES_USER: str = Field(default="postgres")
    POSTGRES_PASSWORD: str = Field(default="admin")
    POSTGRES_HOST: str = Field(default="localhost")
    POSTGRES_PORT: int = Field(default=5432)
    POSTGRES_DB: str = Field(default="spacex_db")
    
    # --- DYNAMIC DATABASE URL ---
    # Usamos @computed_field para garantir que a URL seja gerada 
    # com os valores ATUAIS das variáveis de ambiente no momento da execução.
    @computed_field
    @property
    def DATABASE_URL(self) -> str:
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    model_config = SettingsConfigDict(
        env_file=".env", 
        extra="ignore",
        env_ignore_empty=True # Ignora variáveis vazias no .env
    )

settings = Settings()