import pytest
from unittest.mock import patch, MagicMock
from src.utils import monitoring as mon
from prometheus_client import Counter, Histogram


# Fixtures

@pytest.fixture
def mock_settings_no_webhook(monkeypatch):
    """Settings sem webhook configurado"""
    class DummySettings:
        SLACK_WEBHOOK_URL = ""
        LOG_LEVEL = "INFO"
    monkeypatch.setattr(mon, "get_settings", lambda: DummySettings())
    return DummySettings()

@pytest.fixture
def mock_settings_with_webhook(monkeypatch):
    """Settings com webhook configurado"""
    class DummySettings:
        SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/T000/B000/XXXX"
        LOG_LEVEL = "INFO"
    monkeypatch.setattr(mon, "get_settings", lambda: DummySettings())
    return DummySettings()


# Testes de slack_notify

def test_slack_notify_no_webhook_logs_and_returns(mock_settings_no_webhook, caplog):
    """Testa que não faz nada quando webhook não está configurado"""
    with caplog.at_level("DEBUG"):
        result = mon.slack_notify("mensagem teste")
    
    assert result is None

@patch("src.utils.monitoring.requests.post")
def test_slack_notify_success(mock_post, mock_settings_with_webhook):
    """Testa notificação bem-sucedida ao Slack"""
    mock_post.return_value.status_code = 200
    
    mon.slack_notify("Hello World")
    
    mock_post.assert_called_once_with(
        "https://hooks.slack.com/services/T000/B000/XXXX",
        json={"text": "Hello World"},
        timeout=5
    )

@patch("src.utils.monitoring.requests.post")
def test_slack_notify_with_special_characters(mock_post, mock_settings_with_webhook):
    """Testa mensagens com caracteres especiais (emojis, quebras de linha)"""
    mock_post.return_value.status_code = 200
    
    message = "🚀 Launch falhou!\nDetalhes: erro na ignição"
    mon.slack_notify(message)
    
    mock_post.assert_called_once()
    call_args = mock_post.call_args
    assert call_args[1]["json"]["text"] == message

@patch("src.utils.monitoring.requests.post")
@patch("src.utils.monitoring.logger")
def test_slack_notify_handles_request_exception(mock_logger, mock_post, mock_settings_with_webhook):
    """Testa tratamento de erro quando request falha (linha 23-24)"""
    mock_post.side_effect = Exception("Connection timeout")
    
    # Não deve propagar exceção
    mon.slack_notify("mensagem")
    
    # Deve logar warning
    mock_logger.warning.assert_called_once()
    call_args = mock_logger.warning.call_args
    assert "Falha na notificação Slack" in str(call_args)
    assert "Connection timeout" in str(call_args)

@patch("src.utils.monitoring.requests.post")
def test_slack_notify_handles_timeout(mock_post, mock_settings_with_webhook):
    """Testa que timeout de 5 segundos é respeitado"""
    mock_post.return_value.status_code = 200
    
    mon.slack_notify("test")
    
    # Verifica que timeout=5 foi passado
    assert mock_post.call_args[1]["timeout"] == 5


# Testes de start_metrics_server

@patch("src.utils.monitoring.start_http_server")
def test_start_metrics_server_default_port(mock_server):
    """Testa servidor com porta padrão (8000)"""
    mon.start_metrics_server()
    mock_server.assert_called_once_with(8000)

@patch("src.utils.monitoring.start_http_server")
def test_start_metrics_server_custom_port(mock_server):
    """Testa servidor com porta customizada"""
    mon.start_metrics_server(port=9090)
    mock_server.assert_called_once_with(9090)

@patch("src.utils.monitoring.start_http_server")
def test_start_metrics_server_called_multiple_times(mock_server):
    """Testa comportamento se chamado múltiplas vezes"""
    mon.start_metrics_server(port=8000)
    mon.start_metrics_server(port=8000)
    
    assert mock_server.call_count == 2


# Testes das Métricas Prometheus (CORRIGIDOS)

def test_metrics_are_prometheus_types():
    """Testa que métricas são instâncias corretas do prometheus_client"""
   
    assert isinstance(mon.EXTRACT_COUNT, Counter)
    assert isinstance(mon.SILVER_COUNT, Counter)
    assert isinstance(mon.FLOW_TIME, Histogram)

def test_metrics_labels():
    """Testa que métricas têm as labels configuradas corretamente"""
    
    
    # Para Counter, verificamos via collect()
    extract_metrics = list(mon.EXTRACT_COUNT.collect())
    assert len(extract_metrics) > 0
    
    # Verifica se podemos criar uma métrica com a label esperada (não levanta erro)
    counter_with_label = mon.EXTRACT_COUNT.labels(endpoint="launches")
    assert counter_with_label is not None
    
    # Mesmo para SILVER_COUNT
    silver_with_label = mon.SILVER_COUNT.labels(endpoint="rockets")
    assert silver_with_label is not None
    
    # Para FLOW_TIME
    flow_with_label = mon.FLOW_TIME.labels(flow_name="etl_pipeline")
    assert flow_with_label is not None

def test_metrics_increment():
    """Testa que métricas podem ser incrementadas"""
    # Incrementa e não deve levantar erro
    mon.EXTRACT_COUNT.labels(endpoint="test_launches").inc(5)
    mon.SILVER_COUNT.labels(endpoint="test_rockets").inc(3)

def test_histogram_observe():
    """Testa que histogram pode registrar observações"""
    # Não deve levantar erro
    mon.FLOW_TIME.labels(flow_name="test_flow").observe(1.5)
    mon.FLOW_TIME.labels(flow_name="test_flow").observe(2.5)


# Testes de Integração (opcional)

@pytest.mark.integration
@pytest.mark.skip(reason="Requer servidor Prometheus rodando")
def test_start_metrics_server_integration():
    """Teste real que inicia servidor (só rodar manualmente)"""
    import threading
    import time
    import requests
    
    server_thread = threading.Thread(
        target=mon.start_metrics_server, 
        args=(8001,), 
        daemon=True
    )
    server_thread.start()
    time.sleep(0.5)
    
    response = requests.get("http://localhost:8001/metrics")
    assert response.status_code == 200
    assert "extract_count" in response.text