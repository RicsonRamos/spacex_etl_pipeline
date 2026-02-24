# src/utils/metrics_service.py
from prometheus_client import Counter, Summary, start_http_server
import structlog

logger = structlog.get_logger()

class MetricsService:
    """Serviço centralizado para métricas Prometheus."""

    def __init__(self, port: int = 8001):
        self.port = port
        self.process_time_seconds = Summary("etl_process_time_seconds", "Tempo de execução do ETL", ["endpoint"])
        self.rows_loaded = Counter("etl_rows_loaded_total", "Número de linhas carregadas no ETL", ["endpoint"])
        self.failure_count = Counter("etl_failures_total", "Número de falhas no ETL", ["endpoint"])

    def start_server(self):
        start_http_server(self.port)
        logger.info("Prometheus server iniciado", port=self.port)

    def record_loaded(self, endpoint: str, rows: int):
        self.rows_loaded.labels(endpoint=endpoint).inc(rows)
        logger.debug("Linhas carregadas registradas", endpoint=endpoint, rows=rows)

    def record_failure(self, endpoint: str):
        self.failure_count.labels(endpoint=endpoint).inc()
        logger.debug("Falha registrada", endpoint=endpoint)