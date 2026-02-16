import pytest
import os
from sqlalchemy import create_url
from src.load.loader import PostgresLoader


@pytest.fixture(scope="session")
def db_loader():
    """
    Session-scoped fixture: Creates the loader only once for all tests.
    Uses the DATABASE_URL defined in the environment (Docker or GitHub Actions).
    """
    db_url = os.getenv("DATABASE_URL", "postgresql://postgres:admin@localhost:5432/spacex_db")
    loader = PostgresLoader(connection_string=db_url)
    return loader


@pytest.fixture(scope="function")
def db_setup(db_loader):
    """
    Function-scoped fixture: Ensures each test starts with a clean table.
    Prevents data from 'Test A' from interfering with 'Test B' (Isolation).
    """
    # Cleanup logic (Truncate) before each load test
    with db_loader.engine.connect() as conn:
        conn.execute("TRUNCATE TABLE rockets, launches, launchpads RESTART IDENTITY CASCADE;")
        conn.commit()
    return db_loader
