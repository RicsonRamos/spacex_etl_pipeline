import pytest
from unittest import mock
from src.main import spacex_main_pipeline

# Mocking das dependências
@mock.patch('src.utils.monitoring.start_metrics_server')  # Correção do caminho do start_metrics_server
@mock.patch('src.flows.tasks.process_entity_task')  # Correção do caminho da task process_entity_task
@mock.patch('src.flows.flows.run_dbt') # Correção para o caminho correto de run_dbt
def test_spacex_main_pipeline(mock_run_dbt, mock_process_entity_task, mock_start_metrics):
    """
    Teste para o fluxo principal spacex_main_pipeline:
    - Verifica se as tasks 'process_entity_task' são chamadas corretamente.
    - Verifica se o fluxo de dbt é chamado após a ingestão.
    - Verifica a inicialização do servidor de métricas Prometheus.
    """
    
    # Configuração dos mocks
    mock_process_entity_task.return_value = 1  # Simula que 1 registro foi processado com sucesso
    mock_run_dbt.return_value = None  # Não precisamos de retorno para run_dbt
    mock_start_metrics.return_value = None  # Não precisamos de retorno para start_metrics_server

    # Executando o fluxo completo
    spacex_main_pipeline(incremental=False, real_api=True)

    # Verifica se as tasks foram chamadas com os parâmetros corretos
    mock_process_entity_task.assert_any_call("rockets", real_api=True, incremental=False)
    mock_process_entity_task.assert_any_call("launches", real_api=True, incremental=False)

    # Verifica se o fluxo de dbt foi executado após a ingestão de dados
    mock_run_dbt.assert_called_once()

    # Verifica se as métricas foram inicializadas
    mock_start_metrics.assert_called_once_with(8000)

    # Verificar se o fluxo foi chamado corretamente para Rockets e Launches
    assert mock_process_entity_task.call_count == 2  # Espera-se que o processo de Rockets e Launches seja chamado
    assert mock_run_dbt.call_count == 1  # O dbt deve ser executado uma vez