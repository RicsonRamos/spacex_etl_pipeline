import pytest
import requests
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
    
    mocker.patch.object(extractor.session, 'get')
    extractor.session.get.return_value = MagicMock(status_code=500)
    extractor.session.get.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError("Erro 500")

    with pytest.raises(requests.exceptions.HTTPError):
        extractor.fetch_data("rockets")
