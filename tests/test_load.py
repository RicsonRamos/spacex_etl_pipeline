import pytest
import polars as pl
from sqlalchemy import text
from datetime import datetime

def test_upsert_idempotency_behavior(db_connection):
    """
    RIGOR: Valida se o UPSERT evita duplicatas, garantindo que todas
    as colunas obrigatórias do modelo estejam presentes.
    """
    loader = db_connection
    target_id = "idem_test_01"
    
    # 1. SETUP: Inserir dependências (Rockets e Launchpads)
    with loader.engine.begin() as conn:
        conn.execute(text("INSERT INTO silver_rockets (rocket_id, name) VALUES ('r1', 'Falcon 9') ON CONFLICT DO NOTHING"))
        conn.execute(text("INSERT INTO silver_launchpads (launchpad_id, name) VALUES ('lp1', 'VAFB') ON CONFLICT DO NOTHING"))

    # 2. DEFINIÇÃO DE SCHEMA: Garanta que o DF tenha o que o banco espera
    # Se seu model exige 'success' ou 'flight_number', adicione-os aqui!
    data = {
        "launch_id": [target_id],
        "name": ["Original Name"],
        "date_utc": [datetime(2026, 1, 1, 12, 0)],
        "rocket_id": ["r1"],
        "launchpad_id": ["lp1"],
        "success": [True]
    }
    df_original = pl.DataFrame(data)

    # 3. ACT: Primeira carga
    loader.load_to_silver(df_original, table_name="launches", pk_col="launch_id")
    
    # 4. ACT: Segunda carga (Atualização do nome)
    df_updated = df_original.with_columns(pl.lit("Updated Name").alias("name"))
    loader.load_to_silver(df_updated, table_name="launches", pk_col="launch_id")

    # 5. ASSERT: Verificação física rigorosa
    with loader.engine.connect() as conn:
        result = conn.execute(
            text("SELECT name FROM silver_launches WHERE launch_id = :id"),
            {"id": target_id}
        ).mappings().fetchone()
        
        count = conn.execute(
            text("SELECT COUNT(*) FROM silver_launches WHERE launch_id = :id"),
            {"id": target_id}
        ).scalar()

    assert count == 1, f"Deveria existir apenas 1 registro, mas foram encontrados {count}"
    assert result["name"] == "Updated Name", f"O nome deveria ser 'Updated Name', mas é '{result['name']}'"