import pytest
import polars as pl
from sqlalchemy import text, exc
from datetime import datetime

def test_upsert_constraints_not_null(db_connection):
    """
    Deve falhar ao tentar inserir date_utc = NULL.
    """
    loader = db_connection

    invalid_df = pl.DataFrame(
        [{
            "launch_id": "fail_01",
            "name": "Fail Test",
            "date_utc": None,
            "rocket_id": "falcon9",
            "launchpad_id": "vafb_slc_4e"
        }],
        schema={
            "launch_id": pl.String,
            "name": pl.String,
            "date_utc": pl.Datetime,
            "rocket_id": pl.String,
            "launchpad_id": pl.String,
        }
    )

    # Espera IntegrityError do PostgreSQL
    with pytest.raises(exc.IntegrityError):
        loader.upsert_dataframe(
            invalid_df,
            table_name="launches",
            pk_col="launch_id"
        )


def test_upsert_idempotency_behavior(db_connection):
    """
    Deve atualizar registro existente via ON CONFLICT.
    """
    loader = db_connection
    target_id = "idem_test_01"

    # Inserção original
    df_original = pl.DataFrame([{
        "launch_id": target_id,
        "name": "Original Name",
        "date_utc": datetime(2026, 1, 1),
        "rocket_id": "falcon9",
        "launchpad_id": "vafb_slc_4e"
    }])
    loader.upsert_dataframe(df_original, table_name="launches", pk_col="launch_id")

    # Atualização (idempotência)
    df_updated = pl.DataFrame([{
        "launch_id": target_id,
        "name": "Updated Name",
        "date_utc": datetime(2026, 1, 1),
        "rocket_id": "falcon9",
        "launchpad_id": "vafb_slc_4e"
    }])
    loader.upsert_dataframe(df_updated, table_name="launches", pk_col="launch_id")

    # Valida atualização no banco
    with loader.engine.connect() as conn:
        result = conn.execute(
            text("SELECT name FROM launches WHERE launch_id = :id"),
            {"id": target_id}
        ).fetchone()

    assert result is not None
    assert result[0] == "Updated Name"
