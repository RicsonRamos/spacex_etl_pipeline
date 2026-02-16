import os
from typing import Optional
from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Settings for the application.
    """

    #  API CONFIG 
    SPACEX_API_URL: str = Field(
        default="https://api.spacexdata.com/v4",
        description="The base URL of the SpaceX API",
    )
    RETRIES: int = Field(
        default=3,
        description="The number of times to retry a failed request",
    )
    TIMEOUT: int = Field(
        default=10,
        description="The timeout in seconds for a request",
    )

    #  POSTGRES CONFIG 
  
    POSTGRES_USER: str = Field(
        default="postgres",
        description="The username for the PostgreSQL database",
    )
    POSTGRES_PASSWORD: str = Field(
        default="admin",
        description="The password for the PostgreSQL database",
    )
    POSTGRES_HOST: str = Field(
        default="localhost",
        description="The hostname or IP address of the PostgreSQL server",
    )
    POSTGRES_PORT: int = Field(
        default=5432,
        description="The port number of the PostgreSQL server",
    )
    POSTGRES_DB: str = Field(
        default="spacex_db",
        description="The name of the PostgreSQL database",
    )
    
    #  DYNAMIC DATABASE URL 

    @computed_field
    @property
    def DATABASE_URL(self) -> str:
        """
        The dynamic database URL.
        """
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    model_config = SettingsConfigDict(
        env_file=".env", 
        extra="ignore",
        env_ignore_empty=True 
    )

settings = Settings()