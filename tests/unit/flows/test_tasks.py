# tests/unit/flows/test_tasks.py
"""
Testes para tasks do Prefect.
Verifica orquestração de tasks com injeção de dependências.
"""

import pytest
from unittest import mock
import logging

from src.flows.tasks import process_entity_task

# Desativar logs verbosos
logging.getLogger("prefect").setLevel(logging.ERROR)


class TestProcessEntityTask:
    """Testes para a task de processamento de entidades."""

    @mock.patch('src.flows.tasks.ETLFactory.create')
    @mock.patch('src.flows.tasks.metrics')
    def test_process_entity_task_rockets_success(self, mock_metrics, mock_create):
        """Testa processamento bem-sucedido de rockets."""
        # Arrange: Configurar mock do serviço
        mock_service = mock.Mock()
        mock_service.run.return_value = 10
        mock_create.return_value = mock_service

        # Act: Executar task
        result = process_entity_task("rockets", real_api=True, incremental=False)

        # Assert: Verificar comportamento
        mock_create.assert_called_once_with(
            entity="rockets", incremental=False, real_api=True
        )
        mock_service.run.assert_called_once()
        mock_metrics.inc_extract.assert_called_once_with("rockets", 1)
        mock_metrics.inc_silver.assert_called_once_with("rockets", 1)
        assert result == 10

    @mock.patch('src.flows.tasks.ETLFactory.create')
    @mock.patch('src.flows.tasks.metrics')
    def test_process_entity_task_launches_success(self, mock_metrics, mock_create):
        """Testa processamento bem-sucedido de launches."""
        mock_service = mock.Mock()
        mock_service.run.return_value = 5
        mock_create.return_value = mock_service

        result = process_entity_task("launches", real_api=False, incremental=True)

        mock_create.assert_called_once_with(
            entity="launches", incremental=True, real_api=False
        )
        mock_service.run.assert_called_once()
        mock_metrics.inc_extract.assert_called_once_with("launches", 1)
        mock_metrics.inc_silver.assert_called_once_with("launches", 1)
        assert result == 5

    @mock.patch('src.flows.tasks.ETLFactory.create')
    @mock.patch('src.flows.tasks.metrics')
    def test_process_entity_task_service_failure(self, mock_metrics, mock_create):
        """Testa quando o serviço falha."""
        mock_service = mock.Mock()
        mock_service.run.side_effect = Exception("Database connection failed")
        mock_create.return_value = mock_service

        with pytest.raises(Exception, match="Database connection failed"):
            process_entity_task("rockets")
        
        # Métricas não devem ser atualizadas em caso de falha
        mock_metrics.inc_extract.assert_not_called()
        mock_metrics.inc_silver.assert_not_called()

    @mock.patch('src.flows.tasks.ETLFactory.create')
    @mock.patch('src.flows.tasks.metrics')
    def test_process_entity_task_zero_result(self, mock_metrics, mock_create):
        """Testa quando não há dados processados."""
        mock_service = mock.Mock()
        mock_service.run.return_value = 0
        mock_create.return_value = mock_service

        result = process_entity_task("rockets")

        # Métricas são atualizadas mesmo com 0
        mock_metrics.inc_extract.assert_called_once_with("rockets", 1)
        mock_metrics.inc_silver.assert_called_once_with("rockets", 1)
        assert result == 0