import pytest
from unittest import mock
from datetime import datetime
from src.flows.tasks import process_entity_task
from src.utils.monitoring import EXTRACT_COUNT, SILVER_COUNT

@pytest.mark.asyncio
@mock.patch("src.flows.tasks.BronzeLoader")
@mock.patch("src.flows.tasks.SilverLoader")
@mock.patch("src.flows.tasks.WatermarkManager")
@mock.patch("src.flows.tasks.SchemaValidator")
@mock.patch("src.flows.tasks.get_extractor")
@mock.patch("src.flows.tasks.TransformerFactory")
async def test_process_entity_task(mock_transformer_factory, mock_get_extractor,
                                   mock_schema_validator, mock_watermark,
                                   mock_silver_loader, mock_bronze_loader):

    # Mock de instâncias
    extractor_instance = mock.Mock()
    transformer_instance = mock.Mock()
    bronze_loader_instance = mock.Mock()
    silver_loader_instance = mock.Mock()
    watermark_instance = mock.Mock()
    
    # Configurar retornos dos mocks
    mock_get_extractor.return_value = extractor_instance
    mock_transformer_factory.get.return_value = lambda: transformer_instance  # Retorna callable
    mock_bronze_loader.return_value = bronze_loader_instance
    mock_silver_loader.return_value = silver_loader_instance
    mock_watermark.return_value = watermark_instance
    watermark_instance.get_last_ingested.return_value = datetime(2021, 1, 1)

    # Dados de exemplo
    extractor_instance.extract.return_value = [{"id": 1, "name": "rocket1"}]
    transformer_instance.transform.return_value = mock.Mock(is_empty=lambda: False)
    
    # Resetar métricas para não afetar testes anteriores
    EXTRACT_COUNT.labels("rockets")._value.set(0)
    SILVER_COUNT.labels("rockets")._value.set(0)

    # Executa task (modo incremental)
    result = await process_entity_task("rockets", incremental=True)

    # Verificações
    extractor_instance.extract.assert_called_once()
    transformer_instance.transform.assert_called_once()
    bronze_loader_instance.load.assert_called_once_with(
        [{"id": 1, "name": "rocket1"}], entity="rockets", source="spacex_api_v5"
    )
    silver_loader_instance.upsert.assert_called_once()
    assert result > 0