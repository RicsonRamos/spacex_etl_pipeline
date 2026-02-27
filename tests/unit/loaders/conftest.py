import pytest
from src.loaders import base
from src.config.settings import Settings


class TestSettings(Settings):
    """
    Test settings instance with default values.

    This instance is used by all loader modules to avoid PydanticDescriptorProxy.
    """

    # Override default settings values
    POSTGRES_USER: str = "test_user"  # default Postgres user
    POSTGRES_PASSWORD: str = "test_pass"  # default Postgres password
    POSTGRES_HOST: str = "localhost"  # default Postgres host
    POSTGRES_PORT: int = 5432  # default Postgres port
    POSTGRES_DB: str = "testdb"  # default Postgres database name
    SLACK_WEBHOOK_URL: str = None  # default Slack webhook URL
    SPACEX_API_URL: str = "https://api.test.com"  # default SpaceX API URL
    API_RETRIES: int = 1  # default number of retries for API requests
    API_TIMEOUT: int = 5  # default timeout for API requests in seconds


@pytest.fixture(autouse=True)
def patch_settings(monkeypatch):
    """
    Patch the settings instance in all loader modules.

    This fixture ensures that all tests use the same settings instance
    and avoids PydanticDescriptorProxy.

    The settings instance is created using the TestSettings class,
    which is a subclass of Settings with test values.
    """
    test_settings = TestSettings()

    # Patch da instância completa
    monkeypatch.setattr(base, "settings", test_settings)
