import structlog
import logging
import sys
from src.config.settings import settings

def setup_logging():
    """
    Configure the Structlog logging system.

    This function configures the Structlog logging system with the following processors:

    1. Merge context variables
    2. Add log level
    3. Render stack information
    4. Add timestamp
    5. Render output (either to console or JSON)

    The processors are configured based on whether the output is to a console or not.
    If the output is to a console, the ConsoleRenderer is used. Otherwise, the
    JSONRenderer is used, and the dict_tracebacks processor is also added to include
    traceback information in the log output.

    The wrapper class is set to a filtering bound logger, which filters out log
    messages based on the specified log level.
    """
    processors = [
        # Merge context variables
        structlog.contextvars.merge_contextvars,
        # Add log level
        structlog.processors.add_log_level,
        # Render stack information
        structlog.processors.StackInfoRenderer(),
        # Add timestamp
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
    ]

    if sys.stderr.isatty():
        # If the output is to a console, use the ConsoleRenderer
        processors.append(structlog.dev.ConsoleRenderer())
    else:
        # If the output is not to a console, use the JSONRenderer and add traceback information
        processors.append(structlog.processors.JSONRenderer())
        processors.append(structlog.processors.dict_tracebacks)

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(settings.LOG_LEVEL)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


setup_logging()
logger = structlog.get_logger()