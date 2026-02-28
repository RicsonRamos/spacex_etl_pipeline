# tests/unit/extract/conftest.py
"""
Fixtures compartilhadas para testes de extração.
"""

import pytest
from unittest import mock

# ✅ Agora importa de produção, não o contrário!
from src.extract.rockets import DEFAULT_MOCK_ROCKETS
from src.extract.launches import DEFAULT_MOCK_LAUNCHES


@pytest.fixture
def mock_spacex_client():
    """Cliente SpaceX mockado."""
    with mock.patch('src.extract.base.SpaceXAPIClient') as mock_client:
        instance = mock.Mock()
        mock_client.return_value = instance
        yield instance


@pytest.fixture
def mock_rockets_data():
    """Retorna dados mock de foguetes."""
    return DEFAULT_MOCK_ROCKETS.copy()


@pytest.fixture
def mock_launches_data():
    """Retorna dados mock de lançamentos."""
    return DEFAULT_MOCK_LAUNCHES.copy()