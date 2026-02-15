import structlog
import logging
import sys
from src.config.settings import settings

def setup_logging():
    """
    Configura o log estruturado corrigindo o erro de atributo.
    """
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
    ]

    # Lógica de Renderização Condicional
    if sys.stderr.isatty():
        # Ambiente de desenvolvimento (Terminal)
        processors.append(structlog.dev.ConsoleRenderer())
    else:
        # Ambiente de produção (JSON)
        processors.append(structlog.processors.dict_tracebacks)
        processors.append(structlog.processors.JSONRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(settings.LOG_LEVEL)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

# Executa a configuração ao importar
setup_logging()
logger = structlog.get_logger()