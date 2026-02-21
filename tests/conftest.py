import pytest
import polars as pl

@pytest.fixture
def sample_raw_launch():
    """Simula um retorno real da API SpaceX."""
    return {
        "id": "5eb87cd9ffd86e000604b32a",
        "name": "FalconSat",
        "date_utc": "2006-03-24T22:30:00.000Z",
        "success": False,
        "rocket": "5e9d0d95eda69955f709d1eb"
    }

@pytest.fixture
def sample_silver_df():
    """Simula um DataFrame do Polars já processado."""
    return pl.DataFrame({
        "launch_id": ["1"],
        "name": ["Test Launch"],
        "date_utc": ["2026-01-01T00:00:00Z"],
        "success": [True],
        "rocket": ["rocket_id_123"]
    })
