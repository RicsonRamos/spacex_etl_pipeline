from unittest.mock import MagicMock
import polars as pl
from src.load.loader import PostgresLoader

def test_upsert_silver_empty_dataframe():
    """Garante que o loader não quebra e não executa SQL se o DF estiver vazio."""
    loader = PostgresLoader()
    loader.engine = MagicMock() # Mock da conexão
    
    df_empty = pl.DataFrame()
    result = loader.upsert_silver(df_empty, "silver_rockets", "rocket_id")
    
    assert result == 0
    loader.engine.begin.assert_not_called()

def test_load_bronze_calls_execute(pytestconfig):
    """Verifica se o loader tenta executar a query de inserção Bronze."""
    loader = PostgresLoader()
    mock_conn = MagicMock()
    loader.engine.begin.return_value.__enter__.return_value = mock_conn
    
    data = [{"raw": "test"}]
    loader.load_bronze(data, "bronze_test")
    
    # Verifica se o execute foi chamado (a lógica interna de begin().execute())
    assert mock_conn.execute.called