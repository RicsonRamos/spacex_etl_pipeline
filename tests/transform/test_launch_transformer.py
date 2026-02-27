# tests/transform/test_launch_transformer.py

from datetime import datetime
import polars as pl
import pytest

from src.transformers.launch import LaunchTransformer


MOCK_DATA = [
    {
        "launch_id": "1",
        "name": "Falcon 1",
        "date_utc": "2006-03-24T22:30:00.000Z",
        "rocket_id": "falcon1",
    },
    {
        "launch_id": "2",
        "name": "Falcon 9",
        "date_utc": "2010-06-04T18:45:00.000Z",
        "rocket_id": "falcon9",
    },
]


def test_transform_success():
    transformer = LaunchTransformer()

    df = transformer.transform(MOCK_DATA)

    assert isinstance(df, pl.DataFrame)
    assert df.height == 2
    assert set(df.columns) == {"launch_id", "name", "date_utc", "rocket_id"}

def test_transform_empty_input():
    transformer = LaunchTransformer()

    df = transformer.transform([])

    assert df.is_empty()

def test_transform_deduplicates():
    transformer = LaunchTransformer()

    duplicated = MOCK_DATA + [MOCK_DATA[0]]

    df = transformer.transform(duplicated)

    assert df.height == 2

def test_incremental_filter():
    transformer = LaunchTransformer()

    last_ingested = datetime(2008, 1, 1)

    df = transformer.transform(MOCK_DATA, last_ingested=last_ingested)

    # Só Falcon 9 deve passar (2010)
    assert df.height == 1
    assert df["launch_id"][0] == "2"

def test_missing_column_raises_error():
    transformer = LaunchTransformer()

    invalid_data = [
        {
            "launch_id": "1",
            "name": "Falcon 1",
            # faltando rocket_id
        }
    ]

    with pytest.raises(ValueError):
        transformer.transform(invalid_data)
    
def test_cast_types():
    transformer = LaunchTransformer()

    df = transformer.transform(MOCK_DATA)

    assert df.schema["rocket_id"] == pl.Utf8