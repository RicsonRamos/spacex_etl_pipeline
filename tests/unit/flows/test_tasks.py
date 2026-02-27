import pytest
from unittest import mock
from datetime import datetime
import logging
import sys
from io import StringIO
from src.flows.tasks import process_entity_task
from src.utils.monitoring import EXTRACT_COUNT, SILVER_COUNT

import logging
# Desativa imediatamente ao importar
logging.getLogger("prefect").disabled = True
logging.getLogger("rich").disabled = True

@pytest.fixture
def suppress_prefect_io():
    """Substitui stdout/stderr para evitar erro de I/O fechado."""
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    sys.stdout = StringIO()
    sys.stderr = StringIO()
    
    # Desativa logs do prefect
    logging.getLogger("prefect").setLevel(logging.CRITICAL)
    
    yield
    
    sys.stdout = old_stdout
    sys.stderr = old_stderr

@mock.patch("src.flows.tasks.BronzeLoader")
@mock.patch("src.flows.tasks.SilverLoader")
@mock.patch("src.flows.tasks.WatermarkManager")
@mock.patch("src.flows.tasks.SchemaValidator")
@mock.patch("src.flows.tasks.get_extractor")
@mock.patch("src.flows.tasks.TransformerFactory")
def test_process_entity_task(mock_transformer_factory, mock_get_extractor,
                             mock_schema_validator, mock_watermark,
                             mock_silver_loader, mock_bronze_loader, 
                             suppress_prefect_io):

    # Silenciar logs do Prefect
    #caplog.set_level(logging.ERROR, logger="prefect")
    
    # Criar instâncias mock
    extractor_instance = mock.Mock()
    transformer_instance = mock.Mock()
    bronze_loader_instance = mock.Mock()
    silver_loader_instance = mock.Mock()
    watermark_instance = mock.Mock()
    schema_validator_instance = mock.Mock()

    # Configurar factories
    mock_get_extractor.return_value = lambda: extractor_instance
    mock_bronze_loader.return_value = bronze_loader_instance
    mock_silver_loader.return_value = silver_loader_instance
    mock_watermark.return_value = watermark_instance
    mock_schema_validator.return_value = schema_validator_instance
    mock_transformer_factory.create.return_value = transformer_instance

    # Configurar métodos
    watermark_instance.get_last_ingested.return_value = datetime(2021, 1, 1)
    extractor_instance.extract.return_value = [{"id": 1, "name": "rocket1"}]
    
    # DataFrame mock
    df_mock = mock.Mock()
    df_mock.empty = False
    transformer_instance.transform.return_value = df_mock
    
    # ESSENCIAL: retornar int
    silver_loader_instance.upsert.return_value = 1

    # Resetar métricas
    EXTRACT_COUNT.labels("rockets")._value.set(0)
    SILVER_COUNT.labels("rockets")._value.set(0)

    # REMOVER await - chamar diretamente
    result = process_entity_task("rockets", real_api=False, incremental=True)

    # Asserts
    assert result == 1
    extractor_instance.extract.assert_called_once_with(real_api=False)
    bronze_loader_instance.load.assert_called_once_with(
        [{"id": 1, "name": "rocket1"}], 
        entity="rockets", 
        source="spacex_api_v5"
    )
    transformer_instance.transform.assert_called_once()
    silver_loader_instance.upsert.assert_called_once()