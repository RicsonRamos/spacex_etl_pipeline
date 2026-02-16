import pytest
import os
from src.load.loader import PostgresLoader


@pytest.fixture(scope="session")
def db_loader():
    """
    Session-scoped fixture that initializes the PostgresLoader
    using the database URL from the environment.

    In GitHub Actions, the 'postgres' service automatically
    defines the DATABASE_URL environment variable.

    If DATABASE_URL is not set, a default local connection
    string is used.
    """
    # Get database connection string from environment variable
    # Fallback to local PostgreSQL instance if not provided
    return PostgresLoader()


@pytest.fixture(scope="function")
def db_connection(db_loader):
    """
    Function-scoped fixture that resets the database state
    before each test execution.

    It truncates the rockets, launches, and launchpads tables,
    resets auto-incrementing IDs, and cascades to related tables.

    The name 'db_connection' is intentionally used to match
    existing test references and avoid 'fixture not found' errors.
    """
    # Open a database connection using SQLAlchemy engine
    with db_loader.engine.connect() as conn:
        # TRUNCATE removes all data without dropping the tables
        # RESTART IDENTITY resets auto-increment counters
        # CASCADE ensures related dependent records are also removed
        conn.execute(
            "TRUNCATE TABLE rockets, launches, launchpads RESTART IDENTITY CASCADE;"
        )
        conn.commit()

    # Return the loader instance for use in tests
    return db_loader
