# tests/test_alert_service.py
import pytest
from unittest.mock import patch, MagicMock
import requests
from src.utils.alert_service import AlertService


# Fixtures

@pytest.fixture
def alert_service_no_webhook(monkeypatch):
    """AlertService sem webhook configurado"""
    class DummySettings:
        SLACK_WEBHOOK_URL = ""
    monkeypatch.setattr("src.utils.alert_service.get_settings", lambda: DummySettings())
    return AlertService()

@pytest.fixture
def alert_service_with_webhook(monkeypatch):
    """AlertService com webhook configurado"""
    class DummySettings:
        SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/T000/B000/XXXX"
    monkeypatch.setattr("src.utils.alert_service.get_settings", lambda: DummySettings())
    return AlertService()


# Testes de inicialização

def test_alert_service_init_no_webhook(monkeypatch):
    """Testa inicialização quando webhook não está configurado"""
    class DummySettings:
        SLACK_WEBHOOK_URL = ""
    monkeypatch.setattr("src.utils.alert_service.get_settings", lambda: DummySettings())
    
    service = AlertService()
    assert service.webhook_url == ""

def test_alert_service_init_with_webhook(monkeypatch):
    """Testa inicialização quando webhook está configurado"""
    class DummySettings:
        SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/TEST"
    monkeypatch.setattr("src.utils.alert_service.get_settings", lambda: DummySettings())
    
    service = AlertService()
    assert service.webhook_url == "https://hooks.slack.com/services/TEST"


# Testes de alert (sem webhook)

@patch("src.utils.alert_service.logger")
def test_alert_no_webhook_logs_warning(mock_logger, alert_service_no_webhook):
    """Testa que alerta é ignorado e logado quando não há webhook"""
    result = alert_service_no_webhook.alert("Mensagem de teste")
    
    assert result is False
    mock_logger.warning.assert_called_once()
    
   
    call_args = mock_logger.warning.call_args
    assert call_args[0][0] == "Slack webhook não configurado, alerta ignorado"
    assert call_args[1]["message"] == "Mensagem de teste"


# Testes de alert (com webhook)

@patch("src.utils.alert_service.requests.post")
@patch("src.utils.alert_service.logger")
def test_alert_success(mock_logger, mock_post, alert_service_with_webhook):
    """Testa envio bem-sucedido de alerta"""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_post.return_value = mock_response
    
    result = alert_service_with_webhook.alert("🚀 Pipeline concluído!")
    
    assert result is True
    mock_post.assert_called_once_with(
        "https://hooks.slack.com/services/T000/B000/XXXX",
        json={"text": "Pipeline concluído!"}
    )
    
   
    call_args = mock_logger.info.call_args
    assert call_args[0][0] == "Alerta enviado com sucesso"
    assert call_args[1]["message"] == "Pipeline concluído!"

@patch("src.utils.alert_service.requests.post")
@patch("src.utils.alert_service.logger")
def test_alert_with_special_characters(mock_logger, mock_post, alert_service_with_webhook):
    """Testa envio com caracteres especiais e quebras de linha"""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_post.return_value = mock_response
    
    message = "ERRO CRÍTICO\nEndpoint: launches\nDetalhes: Connection timeout"
    alert_service_with_webhook.alert(message)
    
    mock_post.assert_called_once()
    assert mock_post.call_args[1]["json"]["text"] == message

@patch("src.utils.alert_service.requests.post")
@patch("src.utils.alert_service.logger")
def test_alert_http_error(mock_logger, mock_post, alert_service_with_webhook):
    """Testa falha quando HTTP retorna erro (4xx, 5xx)"""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
    mock_post.return_value = mock_response
    
    result = alert_service_with_webhook.alert("Mensagem")
    
    assert result is False
    
    
    call_args = mock_logger.error.call_args
    assert call_args[0][0] == "Falha ao enviar alerta"
    assert call_args[1]["message"] == "Mensagem"
    assert "404 Not Found" in str(call_args[1]["error"])

@patch("src.utils.alert_service.requests.post")
@patch("src.utils.alert_service.logger")
def test_alert_connection_error(mock_logger, mock_post, alert_service_with_webhook):
    """Testa falha quando não consegue conectar (timeout, DNS, etc)"""
    mock_post.side_effect = requests.ConnectionError("Connection timeout")
    
    result = alert_service_with_webhook.alert("Mensagem")
    
    assert result is False
    
    call_args = mock_logger.error.call_args
    assert call_args[1]["message"] == "Mensagem"
    assert "Connection timeout" in str(call_args[1]["error"])

@patch("src.utils.alert_service.requests.post")
@patch("src.utils.alert_service.logger")
def test_alert_timeout_error(mock_logger, mock_post, alert_service_with_webhook):
    """Testa falha por timeout específico"""
    mock_post.side_effect = requests.Timeout("Request timed out")
    
    result = alert_service_with_webhook.alert("Mensagem")
    
    assert result is False

@patch("src.utils.alert_service.requests.post")
@patch("src.utils.alert_service.logger")
def test_alert_generic_request_exception(mock_logger, mock_post, alert_service_with_webhook):
    """Testa falha genérica de RequestException"""
    mock_post.side_effect = requests.RequestException("Generic error")
    
    result = alert_service_with_webhook.alert("Mensagem")
    
    assert result is False