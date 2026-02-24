# tests/test_logging.py
import os
import pytest
from unittest.mock import patch
import structlog

# --- Fixture global para variáveis de ambiente ---
@pytest.fixture(autouse=True, scope="session")
def set_env_vars():
    os.environ["POSTGRES_HOST"] = "localhost"
    os.environ["POSTGRES_PORT"] = "5432"
    os.environ["POSTGRES_USER"] = "postgres"
    os.environ["POSTGRES_PASSWORD"] = "postgres"
    os.environ["POSTGRES_DB"] = "test_db"
    os.environ["LOG_LEVEL"] = "INFO"

# ============================================================
# Testa setup_logging
# ============================================================
@pytest.mark.parametrize("isatty", [True, False])
def test_setup_logging_calls_structlog_configure(isatty):
    with patch("sys.stderr.isatty", return_value=isatty):
        with patch("structlog.configure") as mock_configure:
            # importa aqui depois do patch para garantir que variáveis estão definidas
            import src.utils.logging as logging_module
            logging_module.setup_logging()
            
            # Verifica se configure foi chamado
            assert mock_configure.called
            args, kwargs = mock_configure.call_args
            assert "processors" in kwargs
            assert isinstance(kwargs["processors"], list)

# ============================================================
# Testa se logger é instância Structlog
# ============================================================
def test_logger_is_structlog_logger():
    import src.utils.logging as logging_module
    logger = logging_module.logger
    import structlog

    # O logger pode ser BoundLogger ou BoundLoggerLazyProxy
    allowed_types = (
        structlog._generic.BoundLogger,  # normal
        type(structlog.get_logger()),    # lazy proxy
    )
    assert isinstance(logger, allowed_types)