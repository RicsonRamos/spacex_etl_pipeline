import pytest
import polars as pl
from src.transform.transformer import SpaceXTransformer

@pytest.fixture
def transformer():
    return SpaceXTransformer()

def test_transform_rockets_schema(transformer):
    """Validates that the rockets transformation returns the correct columns and data types."""
    raw_sample = [
        {"id": "5e9d0d95eda69955f709d1eb", "name": "Falcon 1", "type": "rocket", "active": False}
    ]
    
    df = transformer.transform_rockets(raw_sample)
    
    assert isinstance(df, pl.DataFrame)
    assert "rocket_id" in df.columns
    assert df["rocket_id"].dtype == pl.String
    assert df["active"].dtype == pl.Boolean

def test_transform_launches_date_conversion(transformer):
    """Ensures that the UTC date is correctly converted to a Polars Datetime."""
    raw_sample = [{
        "id": "123",
        "name": "Test",
        "date_utc": "2006-03-24T22:30:00.000Z",
        "success": True,
        "flight_number": 1,
        "rocket": "rock123",
        "launchpad": "pad123"
    }]
    
    df = transformer.transform_launches(raw_sample)
    
    # Verifies that the date_utc column is of type Datetime (equivalent to TIMESTAMPTZ in Postgres)
    assert df["date_utc"].dtype in [pl.Datetime, pl.Datetime("ns", "UTC")]
    assert df["launch_year"][0] == 2006
