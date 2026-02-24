import pytest
from testcontainers.postgres import PostgresContainer
from sqlalchemy import create_engine
from src.load.loader import PostgresLoader
import polars as pl
import re

@pytest.fixture(scope="module")
def postgres_container():
    """Sobe um banco real para o teste de integração."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        conn_str = postgres.get_connection_url()
        # Remove o '+psycopg2' caso exista
        conn_str = re.sub(r"\+.*://", "://", conn_str)
        yield conn_str

def test_loader_upsert_integration(postgres_container, sample_silver_df):
    # Arrange: Instancia o loader apontando para o container temporário
    loader = PostgresLoader(connection_string=postgres_container)
    
    # Act: Tenta carregar o dado
    loader.load_silver(sample_silver_df, table_name="silver_launches")
    
    # Assert: Verifica se o dado realmente está lá
    df_check = pl.read_database_uri(
        "SELECT * FROM silver_launches",
        postgres_container
    )
    assert df_check.shape[0] == 1
    assert df_check["name"][0] == "Test Launch"
