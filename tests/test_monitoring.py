# tests/test_monitoring.py
import pytest
from unittest.mock import patch
from src.utils import monitoring as mon

def test_slack_notify_no_webhook(monkeypatch):
    class DummySettings:
        SLACK_WEBHOOK_URL = ""
    monkeypatch.setattr(mon, "get_settings", lambda: DummySettings())
    # Apenas não deve levantar erro
    mon.slack_notify("msg")

@patch("src.utils.monitoring.requests.post")
def test_slack_notify_calls_requests(mock_post, monkeypatch):
    class DummySettings:
        SLACK_WEBHOOK_URL = "http://dummy"
    monkeypatch.setattr(mon, "get_settings", lambda: DummySettings())

    mon.slack_notify("Hello")
    mock_post.assert_called_once_with("http://dummy", json={"text": "Hello"}, timeout=5)

@patch("src.utils.monitoring.start_http_server")
def test_start_metrics_server_calls_http_server(mock_server):
    mon.start_metrics_server(port=1234)
    mock_server.assert_called_once_with(1234)