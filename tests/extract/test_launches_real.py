# tests/integration/test_launches_real.py
import pytest
from src.extract.launches import LaunchExtract

@pytest.mark.integration
def test_launch_extract_real():
    extractor = LaunchExtract()
    data = extractor.extract(real_api=True)
    assert isinstance(data, list)
    assert len(data) > 0
    assert "id" in data[0]
    assert "name" in data[0]