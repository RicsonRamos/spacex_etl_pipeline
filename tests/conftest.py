import pytest
from src.load.loader import PostgresLoader
from src.database.models import Base
from sqlalchemy import text

@pytest.fixture
def db_connection():
    """
    Fixture que retorna um loader com banco limpo e registros mínimos de FK para testes.
    """
    loader = PostgresLoader()

    # Limpa todas as tabelas existentes
    Base.metadata.drop_all(loader.engine)

    # Cria novamente com constraints corretas
    Base.metadata.create_all(loader.engine)

    # Seed mínimo para satisfazer FKs
    with loader.engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO rockets (rocket_id, name, active, stages, cost_per_launch, success_rate_pct)
            VALUES ('falcon9', 'Falcon 9', TRUE, 2, 50000000, 97)
            ON CONFLICT (rocket_id) DO NOTHING
        """))
        conn.execute(text("""
            INSERT INTO launchpads (launchpad_id, name, full_name, locality, region, status)
            VALUES ('vafb_slc_4e', 'VAFB SLC-4E', 'Vandenberg AFB Space Launch Complex 4E', 'California', 'USA', 'active')
            ON CONFLICT (launchpad_id) DO NOTHING
        """))

    yield loader

    # Opcional: limpar banco após testes
    Base.metadata.drop_all(loader.engine)
