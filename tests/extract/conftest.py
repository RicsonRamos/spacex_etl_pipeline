# tests/extract/conftest.py
import pytest
from unittest.mock import patch, MagicMock

MOCK_LAUNCHES = [
    {"id": "1", "name": "Falcon 1", "date_utc": "2006-03-24T22:30:00.000Z", "success": False, "rocket": "falcon1"},
    {"id": "2", "name": "Falcon 9", "date_utc": "2010-06-04T18:45:00.000Z", "success": True, "rocket": "falcon9"},
]

MOCK_ROCKETS = [
    {"id": "falcon1", "name": "Falcon 1", "active": False, "cost_per_launch": 6700000, "success_rate_pct": 40.0},
    {"id": "falcon9", "name": "Falcon 9", "active": True, "cost_per_launch": 50000000, "success_rate_pct": 97.0},
]

@pytest.fixture
def mock_requests_get():
    """Mock genérico de requests.Session.get para todos os extractors"""
    with patch("src.extract.base_extractor.requests.Session.get") as mock_get:
        def side_effect(url, *args, **kwargs):
            mock_response = MagicMock()
            if "launches" in url:
                mock_response.json.return_value = MOCK_LAUNCHES
            elif "rockets" in url:
                mock_response.json.return_value = MOCK_ROCKETS
            else:
                mock_response.json.return_value = []
            mock_response.status_code = 200
            return mock_response
        mock_get.side_effect = side_effect
        yield mock_get