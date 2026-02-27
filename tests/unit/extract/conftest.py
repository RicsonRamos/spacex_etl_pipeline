import pytest
from unittest.mock import MagicMock

MOCK_LAUNCHES = [
    {"id": "1", "name": "Falcon 1", "date_utc": "2006-03-24T22:30:00.000Z", "success": False, "rocket": "falcon1"},
    {"id": "2", "name": "Falcon 9", "date_utc": "2010-06-04T18:45:00.000Z", "success": True, "rocket": "falcon9"},
]

MOCK_ROCKETS = [
    {"id": "falcon1", "name": "Falcon 1", "active": False, "cost_per_launch": 6700000, "success_rate_pct": 40.0},
    {"id": "falcon9", "name": "Falcon 9", "active": True, "cost_per_launch": 50000000, "success_rate_pct": 97.0},
]


@pytest.fixture
def mock_spacex_client():
    """Mock do client da API SpaceX."""
    client = MagicMock()

    def get_side_effect(endpoint):
        if endpoint == "launches":
            return MOCK_LAUNCHES
        elif endpoint == "rockets":
            return MOCK_ROCKETS
        return []

    client.get.side_effect = get_side_effect
    return client