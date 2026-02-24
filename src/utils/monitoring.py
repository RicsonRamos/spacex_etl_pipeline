import structlog
import requests
from prometheus_client import Counter, Histogram, start_http_server
from src.config.settings import get_settings

logger = structlog.get_logger()

# Métricas (isso pode ficar aqui, não depende de env)
EXTRACT_COUNT = Counter("extract_count", "Registros extraídos", ["endpoint"])
SILVER_COUNT = Counter("silver_count", "Registros na Silver", ["endpoint"])
FLOW_TIME = Histogram("flow_duration_seconds", "Tempo de execução", ["flow_name"])

def start_metrics_server(port: int = 8000):
    start_http_server(port)

def slack_notify(msg: str):
    settings = get_settings()  # <-- agora é lazy

    if not settings.SLACK_WEBHOOK_URL:
        return

    try:
        requests.post(settings.SLACK_WEBHOOK_URL, json={"text": msg}, timeout=5)
    except Exception as e:
        logger.warning("Falha na notificação Slack", error=str(e))