import pytest
import polars as pl
from src.transform.transformer import SpaceXTransformer


@pytest.fixture
def transformer():
    return SpaceXTransformer()


def test_transform_rockets_success(transformer):
    """Validates rocket contract and data types."""
    raw_sample = [{
        "id": "5e9d", "name": "Falcon", "type": "rocket", "active": True,
        "stages": 2, "cost_per_launch": 6700000, "success_rate_pct": 40
    }]

    df = transformer.transform_rockets(raw_sample)

    assert df.height == 1
    assert df["rocket_id"][0] == "5e9d"
    assert df["active"].dtype == pl.Boolean
    assert df["cost_per_launch"].dtype == pl.Float64


def test_transform_launches_with_null_success(transformer):
    """Ensures resilience: future launches may have 'success' as null."""
    raw_sample = [{
        "id": "L1", "name": "Future", "date_utc": "2026-03-24T22:30:00Z",
        "success": None, "flight_number": 100, "rocket": "R1", "launchpad": "P1"
    }]

    df = transformer.transform_launches(raw_sample)
    assert df["success"][0] is None


def test_transform_launches_invalid_date(transformer):
    """Ensures invalid date formats raise an exception."""
    raw_sample = [{
        "id": "L2", "name": "Bad Date", "date_utc": "invalid-date",
        "success": True, "flight_number": 101, "rocket": "R1", "launchpad": "P1"
    }]

    with pytest.raises(Exception):
        transformer.transform_launches(raw_sample)


def test_transform_empty_data(transformer):
    """Ensures the transformer handles empty lists safely."""
    df = transformer.transform_rockets([])
    assert df.is_empty()
    assert isinstance(df, pl.DataFrame)


def test_transform_payloads_mass_fill_null(transformer):
    """Validates that null payload masses are converted to zero."""
    raw_sample = [{
        "id": "P1", "name": "Cargo", "type": "Sat", "reused": False,
        "mass_kg": None, "orbit": "LEO"
    }]

    df = transformer.transform_payloads(raw_sample)
    assert df["mass_kg"][0] == 0.0
