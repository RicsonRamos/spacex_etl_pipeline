# tests/extract/test_launches.py
from src.extract.launches import LaunchExtract

def test_launch_extract(mock_requests_get):
    extractor = LaunchExtract()
    data = extractor.extract()

    assert len(data) == 2
    assert data[0]["name"] == "Falcon 1"