# tests/unit/application/test_etl_service.py
"""
Testes para ETLService.
Verifica orquestração completa do pipeline ETL com mocks.
"""

import pytest
from unittest import mock
from datetime import datetime, timezone
import polars as pl

from src.application.etl_service import ETLService


class TestETLService:
    """Testes unitários para o serviço ETL."""
    
    @pytest.fixture
    def mock_dependencies(self):
        """Fixture que fornece todas as dependências mockadas."""
        return {
            'extractor': mock.Mock(),
            'transformer': mock.Mock(),
            'bronze_loader': mock.Mock(),
            'silver_loader': mock.Mock(),
            'watermark': mock.Mock(),
            'metrics': mock.Mock(),
            'notifier': mock.Mock(),
            'schema_validator': mock.Mock(),
        }
    
    @pytest.fixture
    def etl_service(self, mock_dependencies):
        """ETLService com todas as dependências mockadas."""
        return ETLService(
            entity="rockets",
            extractor=mock_dependencies['extractor'],
            transformer=mock_dependencies['transformer'],
            bronze_loader=mock_dependencies['bronze_loader'],
            silver_loader=mock_dependencies['silver_loader'],
            watermark=mock_dependencies['watermark'],
            metrics=mock_dependencies['metrics'],
            notifier=mock_dependencies['notifier'],
            schema_validator=mock_dependencies['schema_validator'],
            incremental=False,
            real_api=False,
        )
    
    def test_init_sets_all_attributes(self, mock_dependencies):
        """Deve inicializar todos os atributos corretamente."""
        service = ETLService(
            entity="launches",
            **mock_dependencies,
            incremental=True,
            real_api=True,
        )
        
        assert service.entity == "launches"
        assert service.incremental is True
        assert service.real_api is True
        assert service.extractor is mock_dependencies['extractor']
        assert service.transformer is mock_dependencies['transformer']
    
    def test_validate_schema_with_expected_schema(self, etl_service, mock_dependencies):
        """Deve validar schema quando expected_schema definido."""
        mock_dependencies['silver_loader'].expected_schema = ["id", "name", "date"]
        
        etl_service._validate_schema()
        
        mock_dependencies['schema_validator'].validate_table_columns.assert_called_once_with(
            table_name="rockets",
            expected_columns=["id", "name", "date"],
        )
    
    def test_validate_schema_without_expected_schema(self, etl_service, mock_dependencies):
        """Deve retornar silenciosamente quando expected_schema não definido."""
        # Remover expected_schema
        if hasattr(mock_dependencies['silver_loader'], 'expected_schema'):
            delattr(mock_dependencies['silver_loader'], 'expected_schema')
        
        # Não deve lançar exceção
        etl_service._validate_schema()
        
        # Não deve chamar validador
        mock_dependencies['schema_validator'].validate_table_columns.assert_not_called()
    
    def test_get_last_ingested_returns_none_when_not_incremental(self, etl_service):
        """Deve retornar None quando não incremental."""
        result = etl_service._get_last_ingested()
        assert result is None
    
    def test_get_last_ingested_with_incremental(self, mock_dependencies):
        """Deve retornar data com timezone quando incremental."""
        service = ETLService(
            entity="rockets",
            **mock_dependencies,
            incremental=True,
            real_api=False,
        )
        
        mock_date = datetime(2024, 1, 1, 12, 0, 0)
        mock_dependencies['watermark'].get_last_ingested.return_value = mock_date
        
        result = service._get_last_ingested()
        
        assert result == mock_date.replace(tzinfo=timezone.utc)
        mock_dependencies['watermark'].get_last_ingested.assert_called_once_with("rockets")
    
    def test_run_full_pipeline_success(self, etl_service, mock_dependencies):
        """Deve executar pipeline completo com sucesso."""
        # Configurar mocks
        raw_data = [{"id": "r1", "name": "Falcon 9"}]
        df_silver = pl.DataFrame({
            "id": ["r1"],
            "name": ["Falcon 9"],
            "date_utc": ["2024-01-01"],
        })
        
        mock_dependencies['extractor'].extract.return_value = raw_data
        mock_dependencies['transformer'].transform.return_value = df_silver
        mock_dependencies['silver_loader'].upsert.return_value = 1
        
        # Executar
        result = etl_service.run()
        
        # Verificar chamadas
        assert result == 1
        
        mock_dependencies['extractor'].extract.assert_called_once_with(real_api=False)
        mock_dependencies['metrics'].inc_extract.assert_called_once_with("rockets", 1)
        mock_dependencies['bronze_loader'].load.assert_called_once_with(
            raw_data,
            entity="rockets",
            source="spacex_api_v5",
        )
        mock_dependencies['transformer'].transform.assert_called_once_with(
            raw_data,
            last_ingested=None,
        )
        mock_dependencies['silver_loader'].upsert.assert_called_once_with(df_silver, entity="rockets")
        mock_dependencies['metrics'].inc_silver.assert_called_once_with("rockets", 1)
    
    def test_run_returns_zero_when_no_data(self, etl_service, mock_dependencies):
        """Deve retornar 0 quando extração retorna vazio."""
        mock_dependencies['extractor'].extract.return_value = []
        
        result = etl_service.run()
        
        assert result == 0
        mock_dependencies['metrics'].inc_extract.assert_not_called()
        mock_dependencies['bronze_loader'].load.assert_not_called()
    
    def test_run_returns_zero_when_transform_empty(self, etl_service, mock_dependencies):
        """Deve retornar 0 quando transformação retorna vazio."""
        raw_data = [{"id": "r1"}]
        empty_df = pl.DataFrame()
        
        mock_dependencies['extractor'].extract.return_value = raw_data
        mock_dependencies['transformer'].transform.return_value = empty_df
        
        result = etl_service.run()
        
        assert result == 0
        mock_dependencies['silver_loader'].upsert.assert_not_called()
    
    def test_run_notifies_and_raises_on_error(self, etl_service, mock_dependencies):
        """Deve notificar e propagar erro em caso de falha."""
        error_msg = "Database connection failed"
        mock_dependencies['extractor'].extract.side_effect = Exception(error_msg)
        
        with pytest.raises(Exception, match=error_msg):
            etl_service.run()
        
        mock_dependencies['notifier'].notify.assert_called_once_with(
            f"Critical pipeline failure SpaceX: rockets - {error_msg}"
        )
    
    def test_run_with_incremental_updates_watermark(self, mock_dependencies):
        """Deve atualizar watermark em modo incremental."""
        service = ETLService(
            entity="rockets",
            **mock_dependencies,
            incremental=True,
            real_api=False,
        )
        
        raw_data = [{"id": "r1"}]
        df_silver = pl.DataFrame({
            "id": ["r1"],
            "date_utc": ["2024-06-15T10:00:00Z"],
        })
        
        mock_dependencies['extractor'].extract.return_value = raw_data
        mock_dependencies['transformer'].transform.return_value = df_silver
        mock_dependencies['silver_loader'].upsert.return_value = 1
        
        service.run()
        
        mock_dependencies['watermark'].update.assert_called_once()
    
    def test_run_skips_watermark_update_when_no_rows(self, mock_dependencies):
        """Não deve atualizar watermark quando nenhuma linha processada."""
        service = ETLService(
            entity="rockets",
            **mock_dependencies,
            incremental=True,
            real_api=False,
        )
        
        raw_data = [{"id": "r1"}]
        df_silver = pl.DataFrame({
            "id": ["r1"],
            "date_utc": ["2024-06-15"],
        })
        
        mock_dependencies['extractor'].extract.return_value = raw_data
        mock_dependencies['transformer'].transform.return_value = df_silver
        mock_dependencies['silver_loader'].upsert.return_value = 0  # Nenhuma linha
        
        service.run()
        
        mock_dependencies['watermark'].update.assert_not_called()
    
    def test_run_with_real_api_flag(self, mock_dependencies):
        """Deve passar real_api para extractor."""
        service = ETLService(
            entity="rockets",
            **mock_dependencies,
            incremental=False,
            real_api=True,
        )
        
        raw_data = [{"id": "r1"}]
        df_silver = pl.DataFrame({"id": ["r1"], "date_utc": ["2024-01-01"]})
        
        mock_dependencies['extractor'].extract.return_value = raw_data
        mock_dependencies['transformer'].transform.return_value = df_silver
        mock_dependencies['silver_loader'].upsert.return_value = 1
        
        service.run()
        
        mock_dependencies['extractor'].extract.assert_called_once_with(real_api=True)