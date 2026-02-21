from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, computed_field

class Settings(BaseSettings):
    """Gestão de configuração via Pydantic V2."""
    
    # API SpaceX
    SPACEX_API_URL: str = "https://api.spacexdata.com/v4"
    API_RETRIES: int = Field(default=3, ge=1)
    API_TIMEOUT: int = Field(default=15, ge=1)

    # Database
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str

    @computed_field
    @property
    def DATABASE_URL(self) -> str:
        """URL de conexão para SQLAlchemy."""
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()