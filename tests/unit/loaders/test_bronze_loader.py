import pytest
from unittest.mock import MagicMock
from src.loaders.bronze_loader import BronzeLoader


@pytest.fixture
def bronze_loader(monkeypatch):
    """
    Fixture to mock the BronzeLoader class.

    This fixture creates a mock of the BronzeLoader class and
    its dependencies. It returns an instance of the
    BronzeLoader class and a mock of the engine.begin()
    context manager.

    The mock of the engine.begin() context manager is used to
    isolate the tests from the actual database.
    """
    loader = BronzeLoader()
    # Mock do context manager engine.begin()
    # This is necessary to isolate the tests from the actual database
    mock_conn = MagicMock()
    # The mock of the context manager is used to intercept the calls to
    # the engine.begin() method and return a mock of the context manager
    mock_context = MagicMock()
    # The mock of the context manager is used to intercept the calls to
    # the engine.begin() method and return a mock of the context manager
    mock_context.execute = MagicMock()
    # The execute method of the context manager is used to intercept the calls
    # to the engine.begin() method and return a mock of the context manager
    mock_conn.__enter__ = lambda s: mock_context
    # The __enter__ method of the context manager is used to intercept the calls
    # to the engine.begin() method and return a mock of the context manager
    mock_conn.__exit__ = lambda s, exc_type, exc_val, exc_tb: None
    # The __exit__ method of the context manager is used to intercept the calls
    # to the engine.begin() method and return a mock of the context manager
    monkeypatch.setattr(loader.engine, "begin", lambda: mock_conn)
    # The begin method of the engine is used to intercept the calls to the
    # engine.begin() method and return a mock of the context manager
    return loader, mock_context


def test_bronze_loader_load(bronze_loader):
    """
    Test the load method of the BronzeLoader class.

    This test ensures that the load method of the BronzeLoader class
    correctly inserts data into the database and returns the number of
    rows inserted.

    Args:
        bronze_loader (Tuple[BronzeLoader, MagicMock]): A fixture that
            returns an instance of the BronzeLoader class and a mock of the
            engine.begin() context manager.

    Returns:
        None
    """
    loader, mock_context = bronze_loader
    data = [{"name": "Falcon 1"}, {"name": "Falcon 9"}]
    # The load method should insert the data into the database and return
    # the number of rows inserted
    rows_inserted = loader.load(data, "bronze_launches", "spacex_api")
    # The number of rows inserted should be equal to the length of the data
    assert rows_inserted == len(data)
    # The execute method of the context manager should be called
    assert mock_context.execute.called
