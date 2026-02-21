import pytest
import polars as pl

@pytest.fixture
def mock_rocket_data():
    return [
        {
            "id": "5e9d0d95eda69955f709d1eb",
            "name": "Falcon 1",
            "type": "rocket",
            "active": False,
            "stages": 2,
            "cost_per_launch": 6700000
        }
    ]

@pytest.fixture
def mock_raw_df(mock_rocket_data):
    return pl.from_dicts(mock_rocket_data)