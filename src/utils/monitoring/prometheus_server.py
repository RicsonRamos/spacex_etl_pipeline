from prometheus_client import start_http_server
import structlog

logger = structlog.get_logger()


class PrometheusServer:
    def __init__(self, port: int = 8000):
        self._port = port
        self._started = False

    def start(self):
        if self._started:
            logger.info("Prometheus já iniciado")
            return

        start_http_server(self._port)
        self._started = True

        logger.info("Servidor Prometheus iniciado", port=self._port)