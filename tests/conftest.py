import pytest
from sqlalchemy import create_engine, text
from src.database.models import Base
from src.config.settings import settings
from src.load.loader import PostgresLoader # Importe seu loader real

@pytest.fixture(scope="session")
def engine():
    engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)
    yield engine
    engine.dispose()

@pytest.fixture(scope="function")
def db_connection(engine):
    """
    Fixture que provê o objeto loader e limpa o banco.
    Substitui a necessidade de criar loader manualmente nos testes.
    """
    _force_cleanup(engine)
    Base.metadata.create_all(engine)
    
    loader = PostgresLoader() # Certifique-se de que ele usa o mesmo engine/settings
    yield loader
    
    _force_cleanup(engine)

def _force_cleanup(engine):
    """
    Remove objetos de forma atômica.
    """
    sync_statements = [
        "DROP VIEW IF EXISTS gold_cost_efficiency_metrics CASCADE",
        "DROP TABLE IF EXISTS silver_payloads CASCADE",
        "DROP TABLE IF EXISTS silver_launches CASCADE",
        "DROP TABLE IF EXISTS silver_rockets CASCADE",
        "DROP TABLE IF EXISTS silver_launchpads CASCADE",
        "DROP TABLE IF EXISTS etl_metrics CASCADE",
        "DROP TABLE IF EXISTS bronze_rockets CASCADE",
        "DROP TABLE IF EXISTS bronze_launches CASCADE",
        "DROP TABLE IF EXISTS bronze_payloads CASCADE",
        "DROP TABLE IF EXISTS bronze_launchpads CASCADE"
    ]
    
    # RIGOR: Cada comando em uma transação separada para evitar aborto em cadeia
    for stmt in sync_statements:
        with engine.connect() as conn:
            try:
                conn.execute(text("COMMIT")) # Garante que não há transação pendente
                conn.execute(text(stmt))
                conn.execute(text("COMMIT"))
            except Exception:
                # Se não puder dropar (ex: lock), passamos para o próximo
                continue