# tests/test_loader_full.py
import polars as pl
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock
from sqlalchemy.exc import SQLAlchemyError

from src.load.loader import PostgresLoader


def build_mock_schema():
    return SimpleNamespace(
        columns=["id", "name", "date_utc", "rocket"],
        pk="id",
        silver_table="launches_silver"
    )


@pytest.fixture
def mock_engine():
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = MagicMock()
    return engine


@pytest.fixture
def loader(mock_engine):
    return PostgresLoader(engine=mock_engine)


def test_upsert_silver_valid(loader):
    df = pl.DataFrame({
        "launch_id": ["1"],
        "name": ["Falcon 9"],
        "date_utc": ["2023-02-01"],
        "rocket": ["falcon9"],
        "success": [True]

    })
    # Patch schema registry
    loader.SCHEMA_REGISTRY = {"launches": build_mock_schema()}
    rows = loader.upsert_silver(df, "launches")
    assert rows == 1
    # Verifica chamadas
    assert loader.engine.begin.return_value.__enter__.return_value.execute.called


def test_upsert_silver_empty_df(loader):
    df = pl.DataFrame()
    loader.SCHEMA_REGISTRY = {"launches": build_mock_schema()}
    rows = loader.upsert_silver(df, "launches")
    assert rows == 0


def test_upsert_silver_schema_mismatch(loader):
    df = pl.DataFrame({"wrong_col": [1]})
    loader.SCHEMA_REGISTRY = {"launches": build_mock_schema()}
    with pytest.raises(ValueError, match="Schema mismatch"):
        loader.upsert_silver(df, "launches")


def test_load_bronze_and_gold(loader):
    df = pl.DataFrame({
        "id": ["1"],            # 1 linha
        "name": ["Falcon 9"]
    })
    # Bronze não vazio
    rows = loader.load_bronze(df, "bronze_table")
    assert rows == 1 
    # Bronze vazio
    empty_df = pl.DataFrame()
    rows = loader.load_bronze(empty_df, "bronze_table")
    assert rows == 0
    # Gold não vazio
    rows = loader.load_gold(df, "gold_table")
    assert rows == 1 
    # Gold vazio
    rows = loader.load_gold(empty_df, "gold_table")
    assert rows == 0


def test_load_silver_with_entity(loader):
    df = pl.DataFrame({
        "launch_id": ["1"],
        "name": ["Falcon 9"],
        "date_utc": ["2023-02-01"],
        "rocket": ["falcon9"],
        "success": [True]
    })
    loader.SCHEMA_REGISTRY = {"launches": build_mock_schema()}
    rows = loader.load_silver(df, entity="launches")
    assert rows == 1


def test_load_silver_with_table_name(loader):
    df = pl.DataFrame({
        "id": ["1"],
        "name": ["Falcon 9"],
        "date_utc": ["2023-02-01"],
        "rocket": ["falcon9"],
        "success": [True]
    })
    rows = loader.load_silver(df, table_name="silver_launches")
    assert rows == 1


def test_validate_schema_missing_entity(loader):
    df = pl.DataFrame({"id": [1], "name": ["Falcon"]})
    with pytest.raises(ValueError, match="not found in SCHEMA_REGISTRY"):
        loader._validate_schema(df, "unknown_entity")


def test_upsert_sqlalchemy_error(loader, mocker):
    df = pl.DataFrame({
        "launch_id": ["1"],
        "name": ["Falcon 9"],
        "date_utc": ["2023-02-01"],
        "rocket": ["falcon9"],
        "success": [True]
    })
    loader.SCHEMA_REGISTRY = {"launches": build_mock_schema()}

    # Forçar exceção ao executar
    mock_conn = loader.engine.begin.return_value.__enter__.return_value
    mocker.patch.object(mock_conn, "execute", side_effect=SQLAlchemyError("DB Error"))

    with pytest.raises(SQLAlchemyError, match="DB Error"):
        loader.upsert_silver(df, "launches")