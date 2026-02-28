import requests
import structlog

from .interfaces import Notifier


class SlackNotifier(Notifier):
    def __init__(self, webhook_url: str | None):
        self._webhook_url = webhook_url
        self._logger = structlog.get_logger()

    def notify(self, message: str) -> None:
        if not self._webhook_url:
            return

        try:
            requests.post(
                self._webhook_url,
                json={"text": message},
                timeout=5,
            )
        except Exception as e:
            self._logger.warning(
                "Falha na notificação Slack",
                error=str(e),
            )