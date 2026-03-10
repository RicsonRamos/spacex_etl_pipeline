import pytest
import pandas as pd
from main import preflight_check

def test_preflight_check_valid_data():
    """Valida que dados íntegros passam no check."""
    data = pd.DataFrame({
        "id": ["123"],
        "flight_number": [1],
        "date_utc": ["2026-01-01T00:00:00Z"]
    })
    assert preflight_check(data, "spacex_launches") is True

def test_preflight_check_missing_column():
    """Valida que a ausência de colunas críticas barra a ingestão."""
    data = pd.DataFrame({
        "id": ["123"],
        # flight_number está ausente
        "date_utc": ["2026-01-01T00:00:00Z"]
    })
    assert preflight_check(data, "spacex_launches") is False

def test_preflight_check_empty_dataframe():
    """Valida que DataFrames vazios são barrados."""
    data = pd.DataFrame()
    assert preflight_check(data, "spacex_launches") is False