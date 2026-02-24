# tests/test_flow.py
from unittest.mock import MagicMock
from prefect.testing.utilities import prefect_test_harness
import pytest

from src.flows.etl_flow import spacex_main_pipeline
from src.config.schema_registry import SCHEMA_REGISTRY

@pytest.mark.integration
def test_flow_logic_integration():
    # Cria mocks
    mock_loader = MagicMock()
    mock_transformer = MagicMock()
    mock_extractor = MagicMock()
    mock_metrics = MagicMock()
    mock_alerts = MagicMock()

    # Mocks
    mock_extractor.fetch.return_value = [{"id": 1, "name": "Test Launch"}]
    mock_transformer.transform.return_value = MagicMock(is_empty=lambda: False)

    with prefect_test_harness():
        state = spacex_main_pipeline(
            loader=mock_loader,
            transformer=mock_transformer,
            extractor=mock_extractor,
            metrics=mock_metrics,
            alerts=mock_alerts,
            run_dbt_flag=False,
            batch_size=10,
            return_state=True,
        )

        # Flow completou
        assert state.is_completed()

        # Verifica chamadas do extractor com batch_size
        for endpoint in SCHEMA_REGISTRY.keys():
            mock_extractor.fetch.assert_any_call(endpoint, batch_size=10)
            mock_transformer.transform.assert_any_call(endpoint, [{"id": 1, "name": "Test Launch"}])

        # Verifica chamadas do loader
        assert mock_loader.load_bronze.called
        assert mock_loader.upsert_silver.called

        # Métricas chamadas
        assert mock_metrics.start_server.called
        assert mock_metrics.record_loaded.called

        # Alertas existem (não necessariamente chamados)
        assert hasattr(mock_alerts, "slack_notify")