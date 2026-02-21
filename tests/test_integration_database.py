import pytest
import polars as pl
from sqlalchemy import text
from testcontainers.postgres import PostgresContainer
from src.load.loader import PostgresLoader
from src.database.models import Base
from sqlalchemy import create_engine

@pytest.fixture(scope="module")
def postgres_container():
    """Sobe um banco Postgres real em um container para o teste."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres

@pytest.fixture
def integration_loader(postgres_container):
    """Configura o loader para apontar para o container temporário de forma limpa."""
    loader = PostgresLoader()
    
    # Obtemos a URL real do container (ex: postgresql://test:test@localhost:54321/test)
    db_url = postgres_container.get_connection_url()
    
    # Criamos uma nova engine específica para o ambiente de teste
    loader.engine = create_engine(db_url)
    
    # Cria as tabelas necessárias no banco temporário
    from src.database.models import Base
    Base.metadata.create_all(loader.engine)
    
    return loader

def test_upsert_silver_integration(integration_loader):
    """
    Testa se o Upsert realmente funciona no banco de dados.
    1. Insere um registro.
    2. Atualiza o mesmo registro e verifica se não houve duplicata.
    """
    table = "silver_rockets"
    pk = "rocket_id"
    
    # Dados Iniciais
    df1 = pl.DataFrame({
        "rocket_id": ["falcon9"],
        "name": ["Falcon 9"],
        "active": [True]
    })
    
    # Primeiro Load
    integration_loader.upsert_silver(df1, table, pk)
    
    # Dados Atualizados (mesmo ID, nome diferente)
    df2 = pl.DataFrame({
        "rocket_id": ["falcon9"],
        "name": ["Falcon 9 Full Thrust"],
        "active": [True]
    })
    
    # Segundo Load (Upsert)
    integration_loader.upsert_silver(df2, table, pk)
    
    # Verificação no Banco Real
    with integration_loader.engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*), name FROM {table} GROUP BY name")).fetchall()
        
        # Rigor: Deve haver apenas 1 linha e o nome deve ser o novo
        assert len(result) == 1
        assert result[0][1] == "Falcon 9 Full Thrust"