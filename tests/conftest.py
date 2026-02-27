# tests/conftest.py
import logging
import pytest

@pytest.fixture(autouse=True)
def suppress_prefect_logging():
    """Desativa logs do Prefect durante os testes."""
    # Desativa todos os handlers do Prefect
    prefect_logger = logging.getLogger("prefect")
    prefect_logger.setLevel(logging.ERROR)
    prefect_logger.handlers = []
    
    # Desativa também o logger raiz do rich/console
    logging.getLogger("rich").setLevel(logging.ERROR)
    
    yield
    
    # Cleanup após o teste
    logging.getLogger("prefect").handlers = []