import pytest
from unittest.mock import MagicMock
import polars as pl
from src.loaders.silver_loader import SilverLoader

@pytest.fixture
def silver_loader(monkeypatch):
    """
    Fixture to test the SilverLoader class.

    Creates a SilverLoader instance and mocks the engine.begin() method.
    The mock returns a context that can be used to assert the execute method was called.
    """
    loader = SilverLoader()
    # Mock engine.begin() -> context do SQLAlchemy
    # The context will be used to assert the execute method was called
    mock_conn = MagicMock()
    mock_context = MagicMock()
    mock_context.execute = MagicMock()
    # Set the __enter__ and __exit__ magic methods of the mock connection
    # to return the mock context
    mock_conn.__enter__ = lambda s: mock_context
    mock_conn.__exit__ = lambda s, exc_type, exc_val, exc_tb: None
    # Replace the engine.begin() method with the mock connection
    monkeypatch.setattr(loader.engine, "begin", lambda: mock_conn)
    return loader, mock_context

def test_silver_loader_upsert(silver_loader):
    """
    Test the upsert method of the SilverLoader class.

    Given a DataFrame with two rows and a table name, the method should insert the two rows
    into the table and return the number of inserted rows.
    """
    loader, mock_context = silver_loader
    # Create a DataFrame with two rows to test the upsert method
    df = pl.DataFrame([{"id": 1, "name": "Falcon 1"}, {"id": 2, "name": "Falcon 9"}])
    # Call the upsert method and assert the number of inserted rows
    rows_inserted = loader.upsert(df, "silver_launches", pk="launches_id")
    assert rows_inserted == len(df)
    # Assert the execute method of the mock context was called
    assert mock_context.execute.called
