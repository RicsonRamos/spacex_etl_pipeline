import os
import polars as pl
import pytest
from sqlalchemy import create_engine
from src.load.loader import PostgresLoader

@pytest.fixture
def sample_raw_launch():
    return {
        "id": "1",
        "name": "Falcon 9",
        "date_utc": "2023-02-01T00:00:00Z",
        "rocket": "falcon9",
        "success": True
    }


@pytest.fixture
def sample_silver_df():
    return pl.DataFrame({
        "id": ["1"],
        "name": ["Test Launch"],
        "date_utc": ["2023-02-01T00:00:00Z"],
        "rocket": ["falcon9"]
    })


@pytest.fixture(scope="session", autouse=True)
def mock_env():
    os.environ["DATABASE_URL"] = "sqlite:///:memory:"
    os.environ["POSTGRES_HOST"] = "localhost"
    os.environ["POSTGRES_PORT"] = "5432"
    os.environ["POSTGRES_USER"] = "postgres"
    os.environ["POSTGRES_PASSWORD"] = "postgres"
    os.environ["POSTGRES_DB"] = "test_db"


@pytest.fixture(scope="session")
def engine():
    engine = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def loader(engine):
    return PostgresLoader(connection_string=os.environ["DATABASE_URL"])