import pytest
from unittest.mock import MagicMock
from src.extract.spacex_api import SpaceXExtractor

def test_extractor_fetch_success(mock_rocket_data):
    extractor = SpaceXExtractor()
    
    # Mock da sessão do requests
    mock_response = MagicMock()
    mock_response.json.return_value = mock_rocket_data
    mock_response.raise_for_status.return_value = None
    extractor.session.get = MagicMock(return_value=mock_response)
    
    data = extractor.fetch("rockets")
    
    assert len(data) == 1
    assert data[0]["name"] == "Falcon 1"