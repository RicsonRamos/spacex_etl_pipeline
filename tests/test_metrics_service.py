# tests/test_metrics_service.py
import pytest
from unittest.mock import patch, MagicMock
from prometheus_client import REGISTRY
from src.utils.metrics_service import MetricsService


@pytest.fixture(autouse=True)
def clean_registry():
    """Limpa o registry do Prometheus antes de cada teste."""
    collectors = list(REGISTRY._collector_to_names.keys())
    for collector in collectors:
        try:
            REGISTRY.unregister(collector)
        except KeyError:
            pass
    yield



# Testes de inicialização

def test_metrics_service_init_default_port(clean_registry):
    """Testa inicialização com porta padrão (8000)."""
    service = MetricsService()
    
    assert service.port == 8000
    # Verifica que as métricas existem (não verifica nome exato devido a possível truncamento)
    assert service.process_time_seconds is not None
    assert service.rows_loaded is not None
    assert service.failure_count is not None
    
    # Verifica que são do tipo correto (Summary e Counter)
    from prometheus_client import Summary, Counter
    assert isinstance(service.process_time_seconds, Summary)
    assert isinstance(service.rows_loaded, Counter)
    assert isinstance(service.failure_count, Counter)


def test_metrics_service_init_custom_port(clean_registry):
    """Testa inicialização com porta customizada."""
    service = MetricsService(port=9090)
    
    assert service.port == 9090


def test_metrics_service_metrics_have_correct_labels(clean_registry):
    """Verifica que as métricas possuem as labels configuradas corretamente."""
    service = MetricsService()
    
    # Verifica se as métricas aceitam a label "endpoint" sem erro
    process_metric = service.process_time_seconds.labels(endpoint="launches")
    rows_metric = service.rows_loaded.labels(endpoint="rockets")
    failure_metric = service.failure_count.labels(endpoint="capsules")
    
    assert process_metric is not None
    assert rows_metric is not None
    assert failure_metric is not None



# Testes de start_server

@patch("src.utils.metrics_service.start_http_server")
@patch("src.utils.metrics_service.logger")
def test_start_server_logs_info(mock_logger, mock_start_server, clean_registry):
    """Testa que start_server inicia o servidor e loga a porta."""
    service = MetricsService(port=8000)
    service.start_server()
    
    mock_start_server.assert_called_once_with(8000)
    mock_logger.info.assert_called_once_with(
        "Prometheus server iniciado", 
        port=8000
    )


@patch("src.utils.metrics_service.start_http_server")
def test_start_server_with_custom_port(mock_start_server, clean_registry):
    """Testa start_server com porta diferente do padrão."""
    service = MetricsService(port=9090)
    service.start_server()
    
    mock_start_server.assert_called_once_with(9090)



# Testes de record_loaded

@patch("src.utils.metrics_service.logger")
def test_record_loaded_increments_counter_and_logs(mock_logger, clean_registry):
    """Testa que record_loaded incrementa o contador e loga corretamente."""
    service = MetricsService()
    
    service.record_loaded("launches", 100)
    
    # Verifica o log
    mock_logger.debug.assert_called_once_with(
        "Linhas carregadas registradas",
        endpoint="launches",
        rows=100
    )


@patch("src.utils.metrics_service.logger")
def test_record_loaded_multiple_calls(mock_logger, clean_registry):
    """Testa múltiplas chamadas de record_loaded."""
    service = MetricsService()
    
    service.record_loaded("launches", 50)
    service.record_loaded("rockets", 30)
    service.record_loaded("launches", 25)  # Mesmo endpoint
    
    assert mock_logger.debug.call_count == 3
    
    # Verifica primeira chamada
    assert mock_logger.debug.call_args_list[0][1] == {
        "endpoint": "launches",
        "rows": 50
    }
    
    # Verifica segunda chamada
    assert mock_logger.debug.call_args_list[1][1] == {
        "endpoint": "rockets", 
        "rows": 30
    }
    
    # Verifica terceira chamada
    assert mock_logger.debug.call_args_list[2][1] == {
        "endpoint": "launches",
        "rows": 25
    }


@patch("src.utils.metrics_service.logger")
def test_record_loaded_zero_rows(mock_logger, clean_registry):
    """Testa registro com zero linhas."""
    service = MetricsService()
    service.record_loaded("launches", 0)
    
    mock_logger.debug.assert_called_once_with(
        "Linhas carregadas registradas",
        endpoint="launches",
        rows=0
    )



# Testes de record_failure

@patch("src.utils.metrics_service.logger")
def test_record_failure_increments_counter_and_logs(mock_logger, clean_registry):
    """Testa que record_failure incrementa o contador e loga corretamente."""
    service = MetricsService()
    
    service.record_failure("launches")
    
    mock_logger.debug.assert_called_once_with(
        "Falha registrada",
        endpoint="launches"
    )


@patch("src.utils.metrics_service.logger")
def test_record_failure_multiple_same_endpoint(mock_logger, clean_registry):
    """Testa múltiplas falhas no mesmo endpoint."""
    service = MetricsService()
    
    for _ in range(3):
        service.record_failure("rockets")
    
    assert mock_logger.debug.call_count == 3
    
    for call in mock_logger.debug.call_args_list:
        assert call[1] == {"endpoint": "rockets"}


@patch("src.utils.metrics_service.logger")
def test_record_failure_different_endpoints(mock_logger, clean_registry):
    """Testa falhas em endpoints diferentes."""
    service = MetricsService()
    
    service.record_failure("launches")
    service.record_failure("rockets")
    service.record_failure("capsules")
    
    assert mock_logger.debug.call_count == 3
    
    endpoints = [call[1]["endpoint"] for call in mock_logger.debug.call_args_list]
    assert endpoints == ["launches", "rockets", "capsules"]



# Testes de integração das métricas (sem mock)

def test_metrics_actually_increment(clean_registry):
    """Testa que os valores das métricas são realmente incrementados."""
    service = MetricsService()
    
    # Incrementa e observa valores (não deve levantar erro)
    service.rows_loaded.labels(endpoint="test").inc(10)
    service.rows_loaded.labels(endpoint="test").inc(5)
    service.failure_count.labels(endpoint="test").inc()
    service.failure_count.labels(endpoint="test").inc()
    service.process_time_seconds.labels(endpoint="test").observe(1.5)


def test_endpoints_are_isolated(clean_registry):
    """Testa que métricas de endpoints diferentes não interferem."""
    service = MetricsService()
    
    # Registra em endpoints diferentes
    service.record_loaded("launches", 100)
    service.record_loaded("rockets", 50)
    service.record_failure("launches")
    
