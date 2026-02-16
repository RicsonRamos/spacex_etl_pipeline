from typing import Optional
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # --- API CONFIG (Adicione estes campos que estavam faltando) ---
    SPACEX_API_URL: str = Field(default="https://api.spacexdata.com/v4")
    RETRIES: int = Field(default=3)  # O erro morre aqui
    TIMEOUT: int = Field(default=10)

    # --- POSTGRES CONFIG ---
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "admin"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "spacex_db"
    
    DATABASE_URL: Optional[str] = None

    @model_validator(mode="after")
    def build_db_url(self) -> "Settings":
        if not self.DATABASE_URL:
            self.DATABASE_URL = (
                f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
                f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
            )
        return self

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()