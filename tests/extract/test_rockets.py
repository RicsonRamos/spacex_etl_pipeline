# tests/extract/test_rockets.py
from src.extract.rockets import RocketExtract

def test_rocket_extract(mock_requests_get):
    extractor = RocketExtract()
    data = extractor.extract()

    assert len(data) == 2
    assert data[1]["active"] is True