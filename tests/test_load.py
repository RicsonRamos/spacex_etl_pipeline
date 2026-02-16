import pytest
import polars as pl
from sqlalchemy import text
from src.load.loader import PostgresLoader

def test_upsert_constraints_not_null(db_connection):
    """
    Valida se o banco rejeita registros sem campos obrigatórios (date_utc).
    Isso testa se o DDL 'NOT NULL' foi aplicado corretamente.
    """
    # Dados inválidos: falta 'date_utc' que definimos como NOT NULL
    invalid_data = pl.DataFrame({
        "launch_id": ["fail_test_01"],
        "name": ["Test Launch Fail"],
        "date_utc": [None]  # Isso deve causar erro
    })

    with pytest.raises(Exception) as excinfo:
        PostgresLoader.upsert_dataframe(invalid_data, "launches", "launch_id")
    
    assert "null value in column" in str(excinfo.value).lower()

def test_upsert_idempotency_behavior(db_connection):
    """
    Testa se a lógica de ON CONFLICT está atualizando em vez de duplicar.
    """
    launch_id = "idempotent_test_01"
    
    # Primeira carga
    df1 = pl.DataFrame({
        "launch_id": [launch_id],
        "name": ["Original Name"],
        "date_utc": ["2026-01-01T00:00:00Z"]
    })
    PostgresLoader.upsert_dataframe(df1, "launches", "launch_id")

    # Segunda carga com o mesmo ID mas nome diferente
    df2 = pl.DataFrame({
        "launch_id": [launch_id],
        "name": ["Updated Name"],
        "date_utc": ["2026-01-01T00:00:00Z"]
    })
    PostgresLoader.upsert_dataframe(df2, "launches", "launch_id")

    # Verificação no banco
    with PostgresLoader.engine.connect() as conn:
        result = conn.execute(
            text("SELECT name FROM launches WHERE launch_id = :id"),
            {"id": launch_id}
        ).fetchone()
        
    assert result[0] == "Updated Name"

def test_data_type_persistence(db_connection):
    """
    Garante que os dados estão sendo salvos com os tipos corretos (ex: BOOLEAN).
    """
    df = pl.DataFrame({
        "launch_id": ["type_test_01"],
        "name": ["Type Test"],
        "date_utc": ["2026-02-16T15:00:00Z"],
        "success": [True] # Polars Boolean -> Postgres Boolean
    })
    
    PostgresLoader.upsert_dataframe(df, "launches", "launch_id")
    
    with PostgresLoader.engine.connect() as conn:
        res = conn.execute(text("SELECT pg_typeof(success) FROM launches WHERE launch_id = 'type_test_01'")).fetchone()
    
    assert res[0] == "boolean"