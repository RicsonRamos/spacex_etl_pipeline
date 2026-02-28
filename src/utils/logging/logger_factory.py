import logging
import structlog

from .interfaces import LoggerConfigurator
from .config import LoggingConfig


class StructlogConfigurator(LoggerConfigurator):
    def __init__(self, config: LoggingConfig):
        self._config = config

    def configure(self) -> None:
        processors = [
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.TimeStamper(
                fmt="%Y-%m-%d %H:%M:%S",
                utc=False,
            ),
        ]

        if self._config.json_output:
            processors.append(structlog.processors.JSONRenderer())
            processors.append(structlog.processors.dict_tracebacks)
        else:
            processors.append(structlog.dev.ConsoleRenderer())

        structlog.configure(
            processors=processors,
            wrapper_class=structlog.make_filtering_bound_logger(
                logging.getLevelName(self._config.log_level)
            ),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=True,
        )