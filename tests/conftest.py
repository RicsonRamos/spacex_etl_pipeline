import os
import polars as pl
import pytest
from sqlalchemy import create_engine
from unittest.mock import MagicMock
from src.load.loader import PostgresLoader


# Fixtures de Dados

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


# Fixtures de Mock (fábricas reutilizáveis)

@pytest.fixture
def mock_etl_components():
    """Factory de mocks para ETL - evita repetição"""
    return {
        "loader": MagicMock(),
        "transformer": MagicMock(),
        "extractor": MagicMock(),
        "metrics": MagicMock(),
        "alerts": MagicMock(),
    }

@pytest.fixture
def dummy_task_factory():
    """Factory de DummyTask para data_quality_task"""
    def _factory(result_value=True):
        class DummyTask:
            def result(self):
                return result_value
        return DummyTask()
    return _factory

@pytest.fixture
def mock_data_quality_task(monkeypatch, dummy_task_factory):
    """Mock global de data_quality_task para todos os testes"""
    def _mock_submit(df, endpoint):
        return dummy_task_factory()
    
    # ⚠️ Mock no módulo onde a função é DEFINIDA, não onde é usada
    monkeypatch.setattr(
        "src.flows.etl_flow.data_quality_task",
        MagicMock(submit=_mock_submit)
    )


# Configuração de Ambiente

@pytest.fixture(scope="session", autouse=True)
def mock_env():
    os.environ.update({
        "DATABASE_URL": "sqlite:///:memory:",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_USER": "postgres",
        "POSTGRES_PASSWORD": "postgres",
        "POSTGRES_DB": "test_db",
        "LOG_LEVEL": "DEBUG",
    })

@pytest.fixture(scope="session")
def engine():
    engine = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)
    yield engine
    engine.dispose()

@pytest.fixture(scope="function")
def loader(engine):
    return PostgresLoader(connection_string=os.environ["DATABASE_URL"])