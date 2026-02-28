# tests/unit/flows/test_flows.py
import pytest
from unittest import mock
import logging
from prefect.testing.utilities import prefect_test_harness

logging.getLogger("prefect").setLevel(logging.ERROR)
logging.getLogger("rich").disabled = True


class TestSpacexMainPipeline:
    """Testes para o flow principal do pipeline SpaceX."""
    
    @pytest.fixture
    def mock_dependencies(self):
        """Fixture que fornece todas as dependências mockadas."""
        with mock.patch('src.flows.flows.process_entity_task') as mock_task, \
             mock.patch('src.flows.flows.run_dbt') as mock_dbt, \
             mock.patch('src.flows.flows.PrometheusServer') as mock_prometheus_class, \
             mock.patch('src.flows.flows.perf_counter') as mock_timer, \
             mock.patch('src.flows.flows.logger') as mock_logger:

            # Configurar mock da task
            mock_future = mock.Mock()
            mock_future.wait.return_value = None
            mock_task.submit.return_value = mock_future

            # Configurar mock do Prometheus
            mock_prometheus = mock.Mock()
            mock_prometheus_class.return_value = mock_prometheus

            # Simular tempo (start=0, end=5.5)
            mock_timer.side_effect = [0, 5.5]

            yield {
                'process_task': mock_task,
                'dbt': mock_dbt,
                'prometheus': mock_prometheus,
                'prometheus_class': mock_prometheus_class,
                'timer': mock_timer,
                'logger': mock_logger,
            }

    def test_pipeline_full_execution_success(self, mock_dependencies):
        """Testa execução completa do pipeline com sucesso."""
        from src.flows.flows import spacex_main_pipeline

        with prefect_test_harness():
            spacex_main_pipeline(incremental=False, real_api=True, enable_metrics=True)

        # Verificar observabilidade
        mock_dependencies['prometheus_class'].assert_called_once_with(port=8000)
        mock_dependencies['prometheus'].start.assert_called_once()

        # Verificar processamento de entidades (ordem correta)
        process_task = mock_dependencies['process_task']
        assert process_task.submit.call_count == 2

        # Primeiro rockets, depois launches
        calls = process_task.submit.call_args_list
        assert calls[0] == mock.call("rockets", real_api=True, incremental=False)
        assert calls[1] == mock.call("launches", real_api=True, incremental=False)

        # Verificar camada gold
        mock_dependencies['dbt'].assert_called_once()
        
        # Verificar log de conclusão
        mock_dependencies['logger'].info.assert_any_call(
            "Pipeline finalizado com sucesso",
            duration_seconds=5.5
        )

    def test_pipeline_with_incremental_mode(self, mock_dependencies):
        """Testa pipeline em modo incremental."""
        from src.flows.flows import spacex_main_pipeline

        with prefect_test_harness():
            spacex_main_pipeline(incremental=True, real_api=False)

        process_task = mock_dependencies['process_task']
        process_task.submit.assert_any_call("rockets", real_api=False, incremental=True)
        process_task.submit.assert_any_call("launches", real_api=False, incremental=True)

    def test_pipeline_without_metrics(self, mock_dependencies):
        """Testa pipeline sem servidor de métricas."""
        from src.flows.flows import spacex_main_pipeline

        with prefect_test_harness():
            spacex_main_pipeline(enable_metrics=False)

        # Prometheus não deve ser iniciado
        mock_dependencies['prometheus_class'].assert_not_called()
        mock_dependencies['prometheus'].start.assert_not_called()
        
        # Mas pipeline deve executar normalmente
        mock_dependencies['process_task'].submit.assert_called()

    def test_pipeline_entities_execution_order(self, mock_dependencies):
        """Testa que entidades são processadas na ordem correta."""
        from src.flows.flows import spacex_main_pipeline

        execution_order = []

        def track_execution(entity, **kwargs):
            execution_order.append(entity)
            future = mock.Mock()
            future.wait.return_value = None
            return future

        mock_dependencies['process_task'].submit.side_effect = track_execution

        with prefect_test_harness():
            spacex_main_pipeline()

        assert execution_order == ["rockets", "launches"]

    def test_pipeline_task_failure_propagation(self, mock_dependencies):
        """Testa que falhas nas tasks são propagadas corretamente."""
        from src.flows.flows import spacex_main_pipeline

        mock_dependencies['process_task'].submit.side_effect = Exception("Task failed")

        with prefect_test_harness():
            with pytest.raises(Exception, match="Task failed"):
                spacex_main_pipeline()


class TestPipelineComponents:
    """Testes unitários para componentes internos do pipeline."""

    def test_start_observability_with_metrics_enabled(self):
        """Testa inicialização de observabilidade quando habilitada."""
        from src.flows.flows import _start_observability

        with mock.patch('src.flows.flows.PrometheusServer') as mock_server_class:
            mock_server = mock.Mock()
            mock_server_class.return_value = mock_server

            _start_observability(enable_metrics=True)

            mock_server_class.assert_called_once_with(port=8000)
            mock_server.start.assert_called_once()

    def test_start_observability_with_metrics_disabled(self):
        """Testa que observabilidade é ignorada quando desabilitada."""
        from src.flows.flows import _start_observability

        with mock.patch('src.flows.flows.PrometheusServer') as mock_server_class:
            _start_observability(enable_metrics=False)
            mock_server_class.assert_not_called()

    def test_run_entities_calls_both_entities(self):
        """Testa que _run_entities processa todas as entidades."""
        from src.flows.flows import _run_entities

        with mock.patch('src.flows.flows.process_entity_task') as mock_task:
            mock_future = mock.Mock()
            mock_future.wait.return_value = None
            mock_task.submit.return_value = mock_future

            _run_entities(incremental=True, real_api=False)

            assert mock_task.submit.call_count == 2
            mock_task.submit.assert_any_call("rockets", real_api=False, incremental=True)
            mock_task.submit.assert_any_call("launches", real_api=False, incremental=True)

    def test_run_gold_layer_executes_dbt(self):
        """Testa que _run_gold_layer chama o dbt."""
        from src.flows.flows import _run_gold_layer

        with mock.patch('src.flows.flows.run_dbt') as mock_dbt:
            _run_gold_layer()
            mock_dbt.assert_called_once()