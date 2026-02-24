import pytest
import polars as pl
from src.flows import data_quality as dq
from datetime import date


# Testes validate_schema

def test_validate_schema_all_columns_present():
    df = pl.DataFrame({"a": [1], "b": [2]})
    assert dq.validate_schema(df, ["a", "b"]) is None

def test_validate_schema_missing_columns():
    df = pl.DataFrame({"a": [1]})
    with pytest.raises(ValueError) as excinfo:
        dq.validate_schema(df, ["a", "b"])
    assert "Missing columns" in str(excinfo.value)


# Testes check_nulls

def test_check_nulls_no_nulls():
    df = pl.DataFrame({"a": [1,2], "b": [3,4]})
    dq.check_nulls(df, ["a", "b"])

def test_check_nulls_with_nulls():
    df = pl.DataFrame({"a": [1, None], "b": [3,4]})
    with pytest.raises(ValueError) as excinfo:
        dq.check_nulls(df, ["a", "b"])
    assert "Null values found" in str(excinfo.value)


# Testes check_duplicates

def test_check_duplicates_no_duplicates():
    df = pl.DataFrame({"a": [1,2], "b": [3,4]})
    dq.check_duplicates(df, ["a", "b"])

def test_check_duplicates_with_duplicates():
    df = pl.DataFrame({"a": [1,1], "b": [3,3]})
    with pytest.raises(ValueError) as excinfo:
        dq.check_duplicates(df, ["a", "b"])
    assert "Duplicate rows found" in str(excinfo.value)


# Testes check_date_ranges

def test_check_date_ranges_all_valid():
    df = pl.DataFrame({"date": [date(2023,1,1), date(2023,1,2)]})
    dq.check_date_ranges(df, "date", date(2023,1,1), date(2023,1,3))

def test_check_date_ranges_invalid():
    df = pl.DataFrame({"date": [date(2023,1,1), date(2023,1,4)]})
    with pytest.raises(ValueError) as excinfo:
        dq.check_date_ranges(df, "date", date(2023,1,1), date(2023,1,3))
    assert "out of expected range" in str(excinfo.value)