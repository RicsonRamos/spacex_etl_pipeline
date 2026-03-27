import pytest
from unittest.mock import Mock, patch
import requests
from ingestion_engine.utils.api_client import SpaceXAPIMocker, NASAAPIMocker, MockAPIResponse

class TestSpaceXAPIMocker:
    """Testes unitários para SpaceXAPIMocker"""

    def test_get_sample_launch(self):
        launch = SpaceXAPIMocker.get_sample_launch()
        assert launch["name"] == "FalconSat"
        assert launch["flight_number"] == 1
        assert "id" in launch

    def test_get_sample_launches_count(self):
        launches = SpaceXAPIMocker.get_sample_launches(count=3)
        assert len(launches) == 3
        assert all("id" in l for l in launches)

    def test_get_rocket_sample(self):
        rocket = SpaceXAPIMocker.get_rocket_sample()
        assert rocket["name"] == "Falcon 1"
        assert rocket["stages"] == 2

class TestNASAAPIMocker:
    """Testes unitários para NASAAPIMocker"""

    def test_get_sample_solar_event(self):
        event = NASAAPIMocker.get_sample_solar_event()
        assert event["eventType"] == "SEP"
        assert "activityID" in event

    def test_get_sample_events_count(self):
        events = NASAAPIMocker.get_sample_events(count=4, event_type="FLR")
        assert len(events) == 4
        assert all(e["eventType"] == "FLR" for e in events)

    def test_get_apod_sample(self):
        apod = NASAAPIMocker.get_apod_sample()
        assert apod["title"] == "Sample Astronomy Picture"
        assert "url" in apod

class TestMockAPIResponse:
    """Testes unitários para MockAPIResponse"""

    def test_success_default(self):
        mock_resp = MockAPIResponse.success(data=[{"id": 1}])
        assert mock_resp.status_code == 200
        assert mock_resp.json() == [{"id": 1}]
        assert "Content-Type" in mock_resp.headers

    def test_error_http(self):
        mock_resp = MockAPIResponse.error(status_code=404, error_message="Not Found")
        assert mock_resp.status_code == 404
        with pytest.raises(requests.HTTPError):
            mock_resp.raise_for_status()

    def test_rate_limit_critical(self):
        mock_resp = MockAPIResponse.rate_limit_critical(remaining=2)
        assert mock_resp.headers["X-RateLimit-Remaining"] == "2"