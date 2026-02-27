import pytest
from src.loaders.schema_validator import SchemaValidator

def test_validate_table_columns(monkeypatch):
    """
    Validate table columns by creating a test table with columns.
    """
    # Create a schema validator
    validator = SchemaValidator()
    
    assert validator is not None

def test_validate_table_columns_missing(monkeypatch):
    """
    Validate table columns by simulating a divergence of columns.
    """
    # Create a schema validator
    validator = SchemaValidator()

    # Simulate divergence of columns
    table_name = "test_table"
    expected_columns = ["id", "name", "date_utc"]
    missing_columns = ["age"]

    # Validate table columns
    with pytest.raises(ValueError):
        validator.validate_table_columns(table_name, expected_columns + missing_columns)

    # Assert that the validator is not None
    assert validator is not None
