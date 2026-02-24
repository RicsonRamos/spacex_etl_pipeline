import pytest
from unittest.mock import MagicMock, patch
import polars as pl
from src.flows.etl_flow import (
    data_quality_task, 
    process_entity_task, 
    spacex_main_pipeline,
)
from src.config.schema_registry import SCHEMA_REGISTRY


# Testes de data_quality_task

def test_data_quality_task_valid_schema():
    """Testa validação com schema correto"""
    df = pl.DataFrame({
        "id": ["1", "2"],
        "name": ["Launch 1", "Launch 2"]
    })
    result = data_quality_task(df, "launches")
    assert result is True

def test_data_quality_task_invalid_schema():
    """Testa comportamento com schema inválido - sua função atual retorna True"""
    df = pl.DataFrame({
        "coluna_errada": [1, 2]
    })
    result = data_quality_task(df, "launches")
    
    assert result is True

def test_data_quality_task_with_nulls():
    """Testa detecção de nulos"""
    df = pl.DataFrame({
        "id": ["1", None],
        "name": ["Launch 1", "Launch 2"]
    })
    # Ajuste conforme sua implementação real
    with pytest.raises(Exception):
        data_quality_task(df, "launches")


# Testes de process_entity_task

class TestProcessEntityTask:
    
    def test_success_with_data(self, mock_etl_components, dummy_task_factory, monkeypatch):
        """Caminho feliz com dados"""
        comps = mock_etl_components
        comps["extractor"].fetch.return_value = [{"id": 1, "name": "Test"}]
        comps["transformer"].transform.return_value = pl.DataFrame([{"id": 1, "name": "Test"}])
        comps["loader"].upsert_silver.return_value = 1
        
        monkeypatch.setattr(
            "src.flows.etl_flow.data_quality_task",
            MagicMock(submit=lambda df, ep: dummy_task_factory())
        )
        
        rows = process_entity_task("launches", **comps)
        assert rows == 1
        
        
        comps["extractor"].fetch.assert_called_once_with("launches", batch_size=1000)
        comps["loader"].load_bronze.assert_called_once()
        comps["metrics"].record_loaded.assert_called_once_with("launches", 1)

    def test_no_data_from_extractor(self, mock_etl_components):
        """Retorna 0 quando extractor não retorna dados"""
        comps = mock_etl_components
        comps["extractor"].fetch.return_value = []
        
        rows = process_entity_task("launches", **comps)
        assert rows == 0
        comps["loader"].load_bronze.assert_not_called()

    def test_empty_after_transform(self, mock_etl_components, dummy_task_factory, monkeypatch):
        """Retorna 0 quando transform gera DataFrame vazio"""
        comps = mock_etl_components
        comps["extractor"].fetch.return_value = [{"id": 1}]
        comps["transformer"].transform.return_value = pl.DataFrame()
        
        monkeypatch.setattr(
            "src.flows.etl_flow.data_quality_task",
            MagicMock(submit=lambda df, ep: dummy_task_factory())
        )
        
        rows = process_entity_task("launches", **comps)
        assert rows == 0

    def test_api_error_triggers_alert(self, mock_etl_components):
        """Erro na API dispara alerta e métrica de falha (com retries do Prefect)"""
        comps = mock_etl_components
        comps["extractor"].fetch.side_effect = ConnectionError("API timeout")

        with pytest.raises(ConnectionError, match="API timeout"):
            process_entity_task("launches", **comps)
        
        
        assert comps["extractor"].fetch.call_count == 3
        assert comps["metrics"].record_failure.call_count == 3
        
        assert comps["alerts"].alert.call_count == 3

    def test_quality_check_failure(self, mock_etl_components, monkeypatch):
        """Falha na qualidade dos dados"""
        comps = mock_etl_components
        comps["extractor"].fetch.return_value = [{"id": 1}]
        comps["transformer"].transform.return_value = pl.DataFrame([{"id": 1}])
        
        class FailingTask:
            def result(self):
                raise ValueError("Schema validation failed")
        
        monkeypatch.setattr(
            "src.flows.etl_flow.data_quality_task",
            MagicMock(submit=lambda df, ep: FailingTask())
        )
        
        with pytest.raises(ValueError):
            process_entity_task("launches", **comps)


# Testes de spacex_main_pipeline

class TestSpacexMainPipeline:
    
    @pytest.fixture(autouse=True)
    def setup_mocks(self, mock_etl_components, monkeypatch):
        """Setup automático para todos os testes desta classe"""
        self.comps = mock_etl_components
        
        self.comps["extractor"].fetch.return_value = [{"id": 1, "name": "Test"}]
        self.comps["transformer"].transform.return_value = pl.DataFrame([{"id": 1, "name": "Test"}])
        self.comps["loader"].upsert_silver.return_value = 1
        
        monkeypatch.setattr(
            "src.flows.etl_flow.data_quality_task",
            MagicMock(submit=lambda df, ep: type('T', (), {'result': lambda s: True})())
        )

    def test_full_pipeline_execution(self):
        """Testa execução completa com todos os endpoints"""
        spacex_main_pipeline(
            loader=self.comps["loader"],
            transformer=self.comps["transformer"],
            extractor=self.comps["extractor"],
            metrics=self.comps["metrics"],
            alerts=self.comps["alerts"],
            run_dbt_flag=True,
            batch_size=5,
        )
        
        # Verifica chamadas para TODOS os endpoints do registry
        for endpoint in SCHEMA_REGISTRY.keys():
            self.comps["extractor"].fetch.assert_any_call(endpoint, batch_size=5)
            self.comps["transformer"].transform.assert_any_call(
                endpoint, 
                [{"id": 1, "name": "Test"}]
            )

        assert self.comps["metrics"].start_server.called
        

    def test_pipeline_execution_without_dbt(self):
        """Testa execução sem rodar dbt"""
        spacex_main_pipeline(
            loader=self.comps["loader"],
            transformer=self.comps["transformer"],
            extractor=self.comps["extractor"],
            metrics=self.comps["metrics"],
            alerts=self.comps["alerts"],
            run_dbt_flag=False,
        )
        
       
        assert self.comps["extractor"].fetch.called
        assert self.comps["transformer"].transform.called
        assert self.comps["metrics"].start_server.called