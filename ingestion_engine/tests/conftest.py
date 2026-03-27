"""
Configurações e fixtures globais para pytest.
Este arquivo é carregado automaticamente pelo pytest.
"""

import os
import sys
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# =============================================================================
# CONFIGURAÇÃO DE PATHS
# =============================================================================

# Adiciona o diretório ingestion_engine ao path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# =============================================================================
# FIXTURES DE AMBIENTE (AUTO-USE)
# =============================================================================

@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """
    Fixture automática para todas as variáveis de ambiente necessárias.
    Executa automaticamente em todos os testes.
    """
    env_vars = {
        "NASA_API_KEY": "test_nasa_key_12345",
        "POSTGRES_USER": "test_user",
        "POSTGRES_PASSWORD": "test_password",
        "POSTGRES_DB": "test_db",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "ALERT_EMAIL": "test@example.com",
        "SLACK_WEBHOOK_URL": "https://hooks.slack.com/test/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
        "AIRFLOW_UID": "1000"
    }
    
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)


@pytest.fixture(autouse=True)
def reset_imports():
    """
    Limpa imports em cache antes de cada teste para evitar
    problemas com mocks de módulos.
    """
    modules_to_clear = [
        'main',
        'src.extractors.concrete_extractors',
        'src.loaders.postgres_loader',
        'src.utils.notifications',
        'config.endpoints'
    ]
    
    for module in modules_to_clear:
        if module in sys.modules:
            del sys.modules[module]
    
    yield


# =============================================================================
# FIXTURES DE DADOS (SpaceX)
# =============================================================================

@pytest.fixture
def sample_spacex_launch():
    """Retorna um único lançamento SpaceX de exemplo."""
    return {
        "id": "5eb87cd9ffd86e000604b32a",
        "flight_number": 1,
        "name": "FalconSat",
        "date_utc": "2006-03-24T22:30:00.000Z",
        "date_unix": 1143239400,
        "success": False,
        "rocket": "5e9d0d95eda69955f709d1eb",
        "details": "Engine failure at 33 seconds and loss of vehicle"
    }


@pytest.fixture
def sample_spacex_launches():
    """Retorna múltiplos lançamentos SpaceX de exemplo."""
    return [
        {
            "id": "5eb87cd9ffd86e000604b32a",
            "flight_number": 1,
            "name": "FalconSat",
            "date_utc": "2006-03-24T22:30:00.000Z",
            "success": False,
            "rocket": "5e9d0d95eda69955f709d1eb"
        },
        {
            "id": "5eb87cdaffd86e000604b32b",
            "flight_number": 2,
            "name": "DemoSat",
            "date_utc": "2007-03-21T01:10:00.000Z",
            "success": False,
            "rocket": "5e9d0d95eda69955f709d1eb"
        },
        {
            "id": "5eb87cdbffd86e000604b32c",
            "flight_number": 3,
            "name": "Trailblazer",
            "date_utc": "2008-08-03T03:34:00.000Z",
            "success": False,
            "rocket": "5e9d0d95eda69955f709d1eb"
        }
    ]


@pytest.fixture
def sample_spacex_df(sample_spacex_launches):
    """Retorna DataFrame pandas com dados SpaceX."""
    return pd.DataFrame(sample_spacex_launches)


# =============================================================================
# FIXTURES DE DADOS (NASA)
# =============================================================================

@pytest.fixture
def sample_nasa_event():
    """Retorna um único evento solar NASA de exemplo."""
    return {
        "activityID": "2026-03-27T12:00:00-SEP-001",
        "startTime": "2026-03-27T12:00:00Z",
        "peakTime": "2026-03-27T14:30:00Z",
        "endTime": "2026-03-27T18:00:00Z",
        "eventType": "SEP",
        "classType": "X1.0",
        "sourceLocation": "N20E15",
        "activeRegionNum": 13000,
        "link": "https://kauai.ccmc.gsfc.nasa.gov/DONKI/view/SEP/1/"
    }


@pytest.fixture
def sample_nasa_events():
    """Retorna múltiplos eventos solares NASA de exemplo."""
    return [
        {
            "activityID": "2026-03-27T12:00:00-SEP-001",
            "startTime": "2026-03-27T12:00:00Z",
            "eventType": "SEP"
        },
        {
            "activityID": "2026-03-28T08:30:00-FLR-001",
            "startTime": "2026-03-28T08:30:00Z",
            "eventType": "FLR"
        },
        {
            "activityID": "2026-03-29T15:45:00-CME-001",
            "startTime": "2026-03-29T15:45:00Z",
            "eventType": "CME"
        }
    ]


@pytest.fixture
def sample_nasa_df(sample_nasa_events):
    """Retorna DataFrame pandas com dados NASA."""
    return pd.DataFrame(sample_nasa_events)


@pytest.fixture
def nasa_nested_response(sample_nasa_events):
    """Retorna resposta aninhada típica da API NASA."""
    return {
        "data": {
            "events": sample_nasa_events,
            "count": len(sample_nasa_events),
            "page": 1
        },
        "meta": {
            "total": len(sample_nasa_events),
            "limit": 10
        }
    }


# =============================================================================
# FIXTURES DE MOCKS HTTP
# =============================================================================

@pytest.fixture
def mock_response_success():
    """Mock de resposta HTTP 200 bem-sucedida."""
    mock = Mock()
    mock.status_code = 200
    mock.headers = {
        "Content-Type": "application/json",
        "X-RateLimit-Remaining": "100"
    }
    mock.json.return_value = []
    mock.raise_for_status.return_value = None
    return mock


@pytest.fixture
def mock_response_rate_limit_critical():
    """Mock de resposta com rate limit crítico."""
    mock = Mock()
    mock.status_code = 200
    mock.headers = {
        "Content-Type": "application/json",
        "X-RateLimit-Remaining": "3",
        "X-RateLimit-Limit": "100"
    }
    mock.json.return_value = []
    mock.raise_for_status.return_value = None
    return mock


@pytest.fixture
def mock_response_rate_limit_normal():
    """Mock de resposta com rate limit normal."""
    mock = Mock()
    mock.status_code = 200
    mock.headers = {
        "Content-Type": "application/json",
        "X-RateLimit-Remaining": "50"
    }
    mock.json.return_value = []
    mock.raise_for_status.return_value = None
    return mock


@pytest.fixture
def mock_response_no_rate_limit():
    """Mock de resposta sem header de rate limit."""
    mock = Mock()
    mock.status_code = 200
    mock.headers = {"Content-Type": "application/json"}
    mock.json.return_value = []
    mock.raise_for_status.return_value = None
    return mock


@pytest.fixture
def mock_response_empty():
    """Mock de resposta com dados vazios."""
    mock = Mock()
    mock.status_code = 200
    mock.headers = {"X-RateLimit-Remaining": "100"}
    mock.json.return_value = []
    mock.raise_for_status.return_value = None
    return mock


# =============================================================================
# FIXTURES DE MOCKS DE CLASSES
# =============================================================================

@pytest.fixture
def mock_postgres_loader():
    """Mock do PostgresLoader."""
    with patch('main.PostgresLoader') as mock_cls:
        mock_instance = Mock()
        mock_instance.load_bronze.return_value = None
        mock_instance.connection = Mock()
        mock_cls.return_value = mock_instance
        yield mock_cls


@pytest.fixture
def mock_alert_system():
    """Mock do AlertSystem."""
    with patch('main.AlertSystem') as mock_cls:
        mock_instance = Mock()
        mock_instance.notify_critical_failure.return_value = None
        mock_instance.notify_warning.return_value = None
        mock_instance.notify_success.return_value = None
        mock_cls.return_value = mock_instance
        yield mock_cls


# =============================================================================
# FIXTURES DE CONFIGURAÇÃO
# =============================================================================

@pytest.fixture
def endpoints_config():
    """Configuração de endpoints para testes."""
    return {
        "spacex_launches": {
            "url": "https://api.spacexdata.com/v4/launches",
            "layer": "bronze"
        },
        "nasa_solar_events": {
            "url": "https://api.nasa.gov/DONKI/SEP",
            "params": {"api_key": "test_key", "startDate": "2026-01-01"},
            "json_path": "data.events",
            "layer": "bronze"
        },
        "spacex_rockets": {
            "url": "https://api.spacexdata.com/v4/rockets",
            "layer": "bronze"
        }
    }


# =============================================================================
# FIXTURES DE DATAFRAMES DE TESTE
# =============================================================================

@pytest.fixture
def empty_dataframe():
    """DataFrame vazio."""
    return pd.DataFrame()


@pytest.fixture
def invalid_spacex_df():
    """DataFrame SpaceX inválido (faltam colunas críticas)."""
    return pd.DataFrame({
        "name": ["Launch 1", "Launch 2"],
        "success": [True, False]
    })


@pytest.fixture
def valid_spacex_df():
    """DataFrame SpaceX válido."""
    return pd.DataFrame({
        "id": ["1", "2", "3"],
        "flight_number": [1, 2, 3],
        "date_utc": ["2026-01-01", "2026-01-02", "2026-01-03"],
        "name": ["Launch 1", "Launch 2", "Launch 3"],
        "success": [True, False, True]
    })


@pytest.fixture
def valid_nasa_df():
    """DataFrame NASA válido."""
    return pd.DataFrame({
        "activityID": ["SEP-001", "SEP-002", "SEP-003"],
        "startTime": ["2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", "2026-01-03T00:00:00Z"],
        "eventType": ["SEP", "SEP", "FLR"]
    })


@pytest.fixture
def df_with_null_ids():
    """DataFrame com IDs nulos."""
    return pd.DataFrame({
        "id": ["1", None, "3"],
        "flight_number": [1, 2, 3],
        "date_utc": ["2026-01-01", "2026-01-02", "2026-01-03"],
        "name": ["Launch 1", "Launch 2", "Launch 3"]
    })

@pytest.fixture(autouse=True)
def auto_mock_postgres(monkeypatch):
    """
    Auto-mock PostgresLoader para todos os testes que importam main.
    """
    # Cria mock do PostgresLoader
    mock_loader = MagicMock()
    mock_loader.load_bronze.return_value = None
    mock_loader.connection = MagicMock()
    
    # Cria mock da classe
    mock_loader_cls = MagicMock(return_value=mock_loader)
    
    # Patch antes de qualquer import
    monkeypatch.setattr('src.loaders.postgres_loader.PostgresLoader', mock_loader_cls)
    monkeypatch.setattr('src.loaders.postgres_loader.create_engine', MagicMock())
    
    yield mock_loader_cls