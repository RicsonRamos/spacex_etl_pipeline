# src/utils/alert_service.py
import requests
import structlog
from src.config.settings import get_settings

logger = structlog.get_logger()

class AlertService:
    """Serviço centralizado para notificações (Slack)."""

    def __init__(self):
        settings = get_settings()
        self.webhook_url = settings.SLACK_WEBHOOK_URL

    def alert(self, message: str):
        if not self.webhook_url:
            logger.warning("Slack webhook não configurado, alerta ignorado", message=message)
            return False
        try:
            response = requests.post(self.webhook_url, json={"text": message})
            response.raise_for_status()
            logger.info("Alerta enviado com sucesso", message=message)
            return True
        except requests.RequestException as e:
            logger.error("Falha ao enviar alerta", message=message, error=str(e))
            return False