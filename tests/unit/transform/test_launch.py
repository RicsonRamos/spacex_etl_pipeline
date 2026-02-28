import pytest
import polars as pl
from datetime import datetime, timezone

from src.transformers.launch import LaunchTransformer

@pytest.fixture
def sample_launch_data():
    return [
        {"launches_id": "1", "name": "Falcon 1 Test", "date": "2006-03-24T22:30:00.000Z", "rocket_id": "falcon1"},
        {"launches_id": "2", "name": "Falcon 9 Test", "date": "2010-06-04T18:45:00.000Z", "rocket_id": "falcon9"},
    ]

def test_transform_normalizes_dates(sample_launch_data):
    transformer = LaunchTransformer()
    df = transformer.transform(sample_launch_data)
    assert df["date_utc"].dtype == pl.Datetime
    assert all(isinstance(dt, datetime) for dt in df["date_utc"].to_list())

def test_transform_applies_incremental_filter(sample_launch_data):
    transformer = LaunchTransformer()
    last_ingested = datetime(2008, 1, 1, tzinfo=timezone.utc)
    df = transformer.transform(sample_launch_data, last_ingested=last_ingested)
    assert all(dt > last_ingested for dt in df["date_utc"].to_list())

def test_transform_handles_empty_data():
    transformer = LaunchTransformer()
    df = transformer.transform([])
    assert df.height == 0