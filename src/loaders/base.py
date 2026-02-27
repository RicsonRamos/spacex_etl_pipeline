import structlog
from sqlalchemy import create_engine
from src.config.settings import settings  # sempre do base.py

logger = structlog.get_logger()


class DatabaseConnection:
    """Shared SQLAlchemy engine.

    This class creates a shared SQLAlchemy engine based on the
    DATABASE_URL setting. It uses a pool of connections to
    improve performance.

    :param None: No parameters are needed.
    :return None: No return value.
    """
    def __init__(self):
        self.engine = create_engine(
            settings.DATABASE_URL,  # use the DATABASE_URL setting
            pool_pre_ping=True,  # pre-ping connections to check for DB availability
            pool_size=10,  # number of connections to maintain in the pool
            max_overflow=20,  # maximum number of connections to allow
            future=True,  # use the new (2.x) API
        )
