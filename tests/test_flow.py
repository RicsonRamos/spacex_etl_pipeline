import pytest
from unittest.mock import MagicMock, patch
import os
import polars as pl
from src.transform.transformer import SpaceXTransformer
from src.load.loader import PostgresLoader

os.environ["PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW"] = "ignore"

# -----------------------------
# Fixtures de dados
# -----------------------------
@pytest.fixture
def sample_launches():
    return [
        {"id": "1", "date_utc": "2026-02-21T12:00:00Z", "name": "Test Launch"},
        {"id": "2", "date_utc": "2026-02-22T15:30:00Z", "name": "Second Launch"},
    ]

@pytest.fixture
def transformer():
    return SpaceXTransformer()

@pytest.fixture
def mock_loader():
    with patch("src.load.loader.create_engine") as mock_create_engine:
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        loader = PostgresLoader()
        loader.engine = mock_engine
        yield loader

# -----------------------------
# Teste pipeline Bronze
# -----------------------------
def test_load_bronze_task(mock_loader, sample_launches, transformer):
    df = transformer.transform("launches", sample_launches)
    records = df.to_dicts()

    mock_conn = MagicMock()
    mock_loader.engine.begin.return_value.__enter__.return_value = mock_conn

    rows_inserted = mock_loader.load_bronze(records, "bronze_table")
    assert rows_inserted == len(records)

# -----------------------------
# Teste pipeline Silver
# -----------------------------
def test_load_silver_task(mock_loader, sample_launches, transformer):
    df = transformer.transform("launches", sample_launches)

    mock_conn = MagicMock()
    mock_loader.engine.begin.return_value.__enter__.return_value = mock_conn

    rows_upserted = mock_loader.upsert_silver(df, "silver_table", "launch_id")
    assert rows_upserted == df.height

# -----------------------------
# Teste pipeline Gold (view)
# -----------------------------
def test_refresh_gold_view(mock_loader):
    mock_conn = MagicMock()
    mock_loader.engine.begin.return_value.__enter__.return_value = mock_conn

    view_def = "SELECT * FROM silver_table"
    mock_loader.refresh_gold_view("gold_view", view_def)
    assert mock_conn.execute.call_count == 2

# -----------------------------
# Teste pipeline completo (Bronze → Silver → Gold)
# -----------------------------
def test_full_pipeline(mock_loader, sample_launches, transformer):
    df = transformer.transform("launches", sample_launches)
    records = df.to_dicts()

    mock_conn = MagicMock()
    mock_loader.engine.begin.return_value.__enter__.return_value = mock_conn

    # Bronze
    rows_bronze = mock_loader.load_bronze(records, "bronze_table")
    assert rows_bronze == len(records)

    # Silver
    rows_silver = mock_loader.upsert_silver(df, "silver_table", "launch_id")
    assert rows_silver == df.height

    # Gold
    view_def = "SELECT * FROM silver_table"
    mock_loader.refresh_gold_view("gold_view", view_def)
    # 2 chamadas executadas no Gold + 2 nas etapas anteriores
    assert mock_conn.execute.call_count == 4