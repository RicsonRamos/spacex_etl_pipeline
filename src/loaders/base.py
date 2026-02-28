import structlog
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

logger = structlog.get_logger()


class DatabaseConnection:
    """
    Base class para acesso a banco via SQLAlchemy.

    Permite injetar DATABASE_URL para facilitar testes.
    """

    def __init__(self, database_url: str):
        if not database_url:
            raise ValueError("Database URL must be provided")
        
        self.engine: Engine = create_engine(
            database_url,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
            future=True,
        )