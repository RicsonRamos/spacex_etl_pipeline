import pytest
from unittest.mock import MagicMock
from src.extract.spacex_api import SpaceXExtractor

@pytest.fixture
def extractor():
    return SpaceXExtractor()

def test_fetch_data_success(extractor, mocker):
    """Tests whether the extractor returns a list when the API responds with 200."""
    mock_response = [{"id": "1", "name": "Falcon 1"}]
    mocker.patch("requests.get", return_value=MagicMock(status_code=200, json=lambda: mock_response))
    
    data = extractor.fetch_data("rockets")
    
    assert isinstance(data, list)
    assert data[0]["name"] == "Falcon 1"

def test_fetch_data_error_handling(extractor, mocker):
    """Tests whether the system raises an exception when the API returns a 500 error."""
    mocker.patch("requests.get", return_value=MagicMock(status_code=500))
    
    with pytest.raises(Exception):
        extractor.fetch_data("rockets")
