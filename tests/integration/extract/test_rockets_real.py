# tests/integration/test_launches_real.py
import pytest
from src.extract.rockets import RocketExtract

@pytest.mark.integration
def test_rockets_extract_real():
    extractor = RocketExtract()
    data = extractor.extract(real_api=True)
    assert isinstance(data, list)
    assert len(data) > 0
    assert "id" in data[0]
    assert "name" in data[0]