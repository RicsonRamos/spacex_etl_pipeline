# src/utils/logging.py
import logging
import sys
import structlog

# Não importa get_settings no topo
# do módulo para evitar falhas de importação
# do Pydantic em testes sem variáveis de ambiente
def setup_logging():
    """
    Configura logging estruturado via structlog.
    Retorna um BoundLogger pronto para uso.
    """
    try:
        from src.config.settings import get_settings
        settings = get_settings()
        log_level = getattr(settings, "LOG_LEVEL", "INFO").upper()
    except Exception:
        # fallback seguro em testes ou se settings não estiver configurado
        log_level = "INFO"

    # Configura logger do stdlib
    logging.basicConfig(
        level=log_level,
        format="%(message)s",
        stream=sys.stdout,
    )

    # Configura structlog
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,           # adiciona nível
            structlog.processors.TimeStamper(fmt="iso"), # timestamp ISO
            structlog.processors.StackInfoRenderer(),    # stack info
            structlog.processors.format_exc_info,        # exceções
            structlog.processors.JSONRenderer()          # saída JSON
        ],
        wrapper_class=structlog.stdlib.BoundLogger,     # tipo de logger esperado nos testes
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger()


# Logger global
logger = setup_logging()