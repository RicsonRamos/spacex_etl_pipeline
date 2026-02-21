import pytest
import polars as pl
from src.transform.transformer import SpaceXTransformer

@pytest.fixture
def sample_launches():
    return [
        {"id": "1", "date_utc": "2026-02-21T12:00:00Z", "name": "Test Launch"},
        {"id": "2", "date_utc": "2026-02-22T15:30:00Z", "name": "Second Launch"},
    ]

@pytest.fixture
def sample_rockets():
    return [
        {"id": "r1", "name": "Falcon 9", "active": True, "cost_per_launch": 50000000, "success_rate_pct": 98.0},
        {"id": "r2", "name": "Falcon Heavy", "active": True, "cost_per_launch": 90000000, "success_rate_pct": 95.0},
    ]

@pytest.fixture
def sample_launchpads():
    return [
        {"id": "lp1", "full_name": "Launch Complex 39A", "status": "active", "rockets": ["r1", "r2"]},
        {"id": "lp2", "full_name": "Vandenberg SLC-4E", "status": "active", "rockets": ["r1"]},
    ]

def test_transform_launches(sample_launches):
    transformer = SpaceXTransformer()
    df = transformer.transform("launches", sample_launches)
    
    assert isinstance(df, pl.DataFrame)
    assert "launch_id" in df.columns
    assert "launch_year" in df.columns
    assert df.height == 2
    assert df["launch_year"].to_list() == [2026, 2026]

def test_transform_rockets(sample_rockets):
    transformer = SpaceXTransformer()
    df = transformer.transform("rockets", sample_rockets)
    
    assert isinstance(df, pl.DataFrame)
    assert "rocket_id" in df.columns
    assert df["name"].to_list() == ["Falcon 9", "Falcon Heavy"]
    assert df["active"].to_list() == [True, True]
    assert df["cost_per_launch"].to_list() == [50000000, 90000000]
    assert df["success_rate_pct"].to_list() == [98.0, 95.0]

def test_transform_launchpads(sample_launchpads):
    transformer = SpaceXTransformer()
    df = transformer.transform("launchpads", sample_launchpads)
    
    assert isinstance(df, pl.DataFrame)
    assert "launchpad_id" in df.columns
    assert "full_name" in df.columns
    assert "rockets" in df.columns
    assert df.height == 2
    assert df["rockets"].to_list() == [["r1", "r2"], ["r1"]]

def test_transform_empty_data():
    transformer = SpaceXTransformer()
    df = transformer.transform("launches", [])
    assert isinstance(df, pl.DataFrame)
    assert df.is_empty()

def test_transform_unknown_endpoint():
    transformer = SpaceXTransformer()
    data = [{"id": "x1", "foo": "bar"}]
    df = transformer.transform("unknown", data)
    # Para endpoint desconhecido, deve renomear id -> unknown_id
    assert "unknown_id" in df.columns