import pytest
from unittest.mock import MagicMock, patch
import polars as pl
from src.load.loader import PostgresLoader
import os

os.environ["PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW"] = "ignore"
# -----------------------------
# Fixture do loader com engine mock
# -----------------------------
@pytest.fixture
def mock_loader():
    with patch("src.load.loader.create_engine") as mock_create_engine:
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        loader = PostgresLoader()
        loader.engine = mock_engine  # sobrescreve o engine real
        yield loader

# -----------------------------
# Teste load_bronze
# -----------------------------
def test_load_bronze_task(mock_loader):
    data = [{"launch_id": 1, "date_utc": "2026-02-21"}]

    mock_conn = MagicMock()
    mock_loader.engine.begin.return_value.__enter__.return_value = mock_conn

    rows = mock_loader.load_bronze(data, "bronze_table")
    assert rows == len(data)
    mock_conn.execute.assert_called()

def test_load_bronze_empty(mock_loader):
    rows = mock_loader.load_bronze([], "bronze_table")
    assert rows == 0

# -----------------------------
# Teste validate_launches
# -----------------------------
def test_validate_launches_success():
    data = [{"launch_id": 1, "date_utc": "2026-02-21"}]
    validated = PostgresLoader.validate_launches(data)
    assert validated == data

def test_validate_launches_missing_launch_id():
    data = [{"date_utc": "2026-02-21"}]
    with pytest.raises(ValueError, match="Missing launch_id"):
        PostgresLoader.validate_launches(data)

def test_validate_launches_missing_date_utc():
    data = [{"launch_id": 1}]
    with pytest.raises(ValueError, match="Missing date_utc"):
        PostgresLoader.validate_launches(data)

# -----------------------------
# Teste upsert_silver
# -----------------------------
@pytest.fixture
def sample_df():
    return pl.DataFrame(
        {
            "launch_id": [1, 2],
            "name": ["Test Launch", "Second Launch"],
            "metadata": [{"key": "value"}, {"key": "value2"}],
        }
    )

def test_upsert_silver_task(mock_loader, sample_df):
    mock_conn = MagicMock()
    mock_loader.engine.begin.return_value.__enter__.return_value = mock_conn

    rows = mock_loader.upsert_silver(sample_df, "silver_table", "launch_id")
    assert rows == sample_df.height
    mock_conn.execute.assert_called()

def test_upsert_silver_empty_df(mock_loader):
    empty_df = pl.DataFrame({"launch_id": [], "name": []})
    rows = mock_loader.upsert_silver(empty_df, "silver_table", "launch_id")
    assert rows == 0

def test_upsert_silver_missing_pk(mock_loader, sample_df):
    df = sample_df.drop("launch_id")
    with pytest.raises(ValueError, match="Primary key 'launch_id' not found"):
        mock_loader.upsert_silver(df, "silver_table", "launch_id")

# -----------------------------
# Teste refresh_gold_view
# -----------------------------
def test_refresh_gold_view(mock_loader):
    mock_conn = MagicMock()
    mock_loader.engine.begin.return_value.__enter__.return_value = mock_conn

    mock_loader.refresh_gold_view("gold_view", "SELECT * FROM silver_table")
    assert mock_conn.execute.call_count == 2