import pytest
from unittest.mock import MagicMock
from src.loaders.watermark import WatermarkManager
from datetime import datetime, timezone

@pytest.fixture
def watermark_manager(monkeypatch):
    """
    Fixture para o WatermarkManager.

    O fixture substitui o engine do WatermarkManager por um mock,
    que retorna um datetime fixo quando o método scalar() é chamado.
    Isso permite testar o comportamento do WatermarkManager sem
    precisar de uma conexão com o banco de dados.

    Returns:
        WatermarkManager: O objeto WatermarkManager com o engine substituído.
        MagicMock: O contexto manager que substitui o engine.
    """
    manager = WatermarkManager()

    # Mock do context manager engine.begin() ou engine.connect()
    mock_conn = MagicMock()
    mock_context = MagicMock()
    
    # Retorna um datetime fixo quando scalar() é chamado
    # O datetime é fixado em 2026-02-26 12:00:00Z
    mock_context.execute.return_value.scalar.return_value = datetime(2026, 2, 26, 12, 0, 0, tzinfo=timezone.utc)

    # Configura o enter/exit do context manager
    mock_conn.__enter__ = lambda s: mock_context
    mock_conn.__exit__ = lambda s, exc_type, exc_val, exc_tb: None

    # Substitui o engine por um mock
    monkeypatch.setattr(manager, "engine", MagicMock(connect=lambda: mock_conn))
    
    return manager, mock_context
def test_get_last_ingested(watermark_manager):
    """
    Tests the get_last_ingested method of the WatermarkManager.

    Verifies if the method returns a datetime with UTC timezone.
    """
    manager, mock_context = watermark_manager
    
    # Call the method to get the timestamp of the last ingestion
    result = manager.get_last_ingested("silver_launches")
    
    # Verify if the result is a datetime with UTC timezone
    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
