import pytest
import polars as pl
from datetime import datetime, timezone

from src.transformers.rocket import RocketTransformer

@pytest.fixture
def sample_rocket_data():
    return [
        {"rocket_id": "falcon1", "name": "Falcon 1", "active": True, "cost_per_launch": 6700000, "success_rate_pct": 40},
        {"rocket_id": "falcon9", "name": "Falcon 9", "active": True, "cost_per_launch": 50000000, "success_rate_pct": 97},
    ]

def test_transform_returns_dataframe(sample_rocket_data):
    transformer = RocketTransformer()
    df = transformer.transform(sample_rocket_data)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 2
    assert set(df.columns) == set(transformer.schema["columns"])

def test_transform_handles_empty_data():
    transformer = RocketTransformer()
    df = transformer.transform([])
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0

def test_deduplication():
    transformer = RocketTransformer()
    data = [
        {"rocket_id": "falcon1", "name": "Falcon 1", "active": True, "cost_per_launch": 6700000, "success_rate_pct": 40},
        {"rocket_id": "falcon1", "name": "Falcon 1 duplicate", "active": True, "cost_per_launch": 6700000, "success_rate_pct": 40},
    ]
    df = transformer.transform(data)
    assert df.height == 1
    assert df["rocket_id"][0] == "falcon1"