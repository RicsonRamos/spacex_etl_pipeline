import pytest
from unittest import mock
import logging
from prefect.testing.utilities import prefect_test_harness

# Desativar logs verbosos do Prefect
logging.getLogger("prefect").setLevel(logging.ERROR)
logging.getLogger("rich").disabled = True


@pytest.fixture
def mock_dependencies():
    """Fixture que fornece mocks configurados para todas as dependências."""
    with mock.patch('src.flows.tasks.process_entity_task') as mock_task, \
         mock.patch('src.flows.flows.start_pipeline_monitoring_task') as mock_metrics, \
         mock.patch('src.flows.flows.run_dbt_task') as mock_dbt:
        
        # Configura mock da task principal
        mock_future = mock.Mock()
        mock_future.wait.return_value = None
        mock_future.result.return_value = 1
        mock_task.submit.return_value = mock_future
        mock_task.return_value = 1
        
        yield {
            'process_task': mock_task,
            'metrics': mock_metrics,
            'dbt': mock_dbt
        }


def test_spacex_main_pipeline_calls_all_tasks(mock_dependencies):
    """
    Teste que verifica se todas as tasks são chamadas corretamente.
    """
    from src.flows.flows import spacex_main_pipeline
    
    # Executa o flow com dependências mockadas via injeção
    with prefect_test_harness():
        spacex_main_pipeline(
            incremental=False, 
            real_api=True,
            process_entity_task=mock_dependencies['process_task'],
            start_metrics=mock_dependencies['metrics'],
            run_dbt=mock_dependencies['dbt']
        )
    
    # Verifica métricas
    mock_dependencies['metrics'].assert_called_once_with(8000)
    
    # Verifica processamento de entidades (2 chamadas via .submit())
    process_task = mock_dependencies['process_task']
    assert process_task.submit.call_count == 2
    
    # Verifica chamadas específicas
    process_task.submit.assert_any_call(
        "rockets", real_api=True, incremental=False
    )
    process_task.submit.assert_any_call(
        "launches", real_api=True, incremental=False
    )
    
    # Verifica dbt
    mock_dependencies['dbt'].assert_called_once()


def test_spacex_main_pipeline_with_defaults(mock_dependencies):
    """
    Teste que verifica o comportamento com parâmetros default.
    """
    from src.flows.flows import spacex_main_pipeline
    
    with prefect_test_harness():
        spacex_main_pipeline(
            process_entity_task=mock_dependencies['process_task'],
            start_metrics=mock_dependencies['metrics'],
            run_dbt=mock_dependencies['dbt']
        )
    
    # Verifica defaults (incremental=False, real_api=False)
    process_task = mock_dependencies['process_task']
    process_task.submit.assert_any_call(
        "rockets", real_api=False, incremental=False
    )


def test_spacex_main_pipeline_incremental_mode(mock_dependencies):
    """
    Teste específico para modo incremental.
    """
    from src.flows.flows import spacex_main_pipeline
    
    with prefect_test_harness():
        spacex_main_pipeline(
            incremental=True,
            real_api=False,
            process_entity_task=mock_dependencies['process_task'],
            start_metrics=mock_dependencies['metrics'],
            run_dbt=mock_dependencies['dbt']
        )
    
    process_task = mock_dependencies['process_task']
    process_task.submit.assert_any_call(
        "rockets", real_api=False, incremental=True
    )
    process_task.submit.assert_any_call(
        "launches", real_api=False, incremental=True
    )