import pytest
from unittest.mock import MagicMock
from src.extract.rockets import RocketExtract

@pytest.fixture
def mock_client():
    client = MagicMock()
    client.get.side_effect = lambda endpoint: [
        {"id": "1", "name": "Falcon 1", "date_utc": "2006-03-24T22:30:00.000Z", "rocket": "falcon1"},
        {"id": "2", "name": "Falcon 9", "date_utc": "2010-06-04T18:45:00.000Z", "rocket": "falcon9"},
    ] if endpoint == "launches" else []
    return client

def test_rocket_extract(mock_spacex_client):
    extractor = RocketExtract(client=mock_spacex_client)
    data = extractor.extract(real_api=True) 

    assert len(data) == 2
    assert data[0]["name"] == "Falcon 1"
    mock_spacex_client.get.assert_called_once_with("rockets")