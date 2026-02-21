import polars as pl
import pytest
from src.load.postgres_loader import PostgresLoader

def test_loader_schema_mismatch():
    # DataFrame com coluna faltando ou nome errado
    df_erroneo = pl.DataFrame({
        "coluna_inexistente": [1, 2, 3]
    })
    
    loader = PostgresLoader(connection_string="sqlite:///:memory:") # Mock string
    
    # O loader deve levantar uma exceção de validação de schema
    with pytest.raises(ValueError, match="Schema mismatch"):
        loader.load_silver(df_erroneo, table_name="silver_launches")
