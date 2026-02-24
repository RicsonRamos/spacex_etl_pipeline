import polars as pl
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

from src.load.loader import PostgresLoader


def build_mock_schema():
    return SimpleNamespace(
        columns=["id", "name", "date_utc", "rocket"],
        pk="id",
        silver_table="launches_silver"
    )


def test_schema_mismatch(mocker):
    mocker.patch(
        "src.load.loader.SCHEMA_REGISTRY",
        {"launches": build_mock_schema()}
    )

    df = pl.DataFrame({
        "coluna_errada": [1]
    })

    loader = PostgresLoader(engine=MagicMock())

    with pytest.raises(ValueError, match="Schema mismatch"):
        loader.upsert_silver(df, "launches")


def test_upsert_success(mocker):
    mock_schema = build_mock_schema()

    mocker.patch(
        "src.load.loader.SCHEMA_REGISTRY",
        {"launches": mock_schema}
    )

    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    loader = PostgresLoader(engine=mock_engine)

    df = pl.DataFrame({
        "id": ["1"],
        "name": ["Falcon 9"],
        "date_utc": ["2023-02-01"],
        "rocket": ["falcon9"]
    })

    rows = loader.upsert_silver(df, "launches")

    calls = mock_conn.execute.call_args_list
    assert len(calls) == 2


def test_upsert_empty_df(mocker):
    mocker.patch(
        "src.load.loader.SCHEMA_REGISTRY",
        {"launches": build_mock_schema()}
    )

    loader = PostgresLoader(engine=MagicMock())

    df = pl.DataFrame()

    rows = loader.upsert_silver(df, "launches")

    assert rows == 0