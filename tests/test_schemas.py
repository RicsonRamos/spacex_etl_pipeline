import pytest
from pydantic import ValidationError
from src.extract.schemas import LaunchAPI


def test_launch_schema_validation():
    valid_data = {
        "id": "5eb87cd9ffd86e000604b32a",
        "name": "Falcon 1",
        "date_utc": "2006-03-24T22:30:00.000Z",
        "success": False,
        "rocket": "5e9d0d95eda69955f709d1eb"
    }

    launch = LaunchAPI(**valid_data)
    assert launch.id == "5eb87cd9ffd86e000604b32a"


def test_launch_schema_invalid_types():
    invalid_data = {
        "id": 123,
        "name": "Falcon 1",
        "success": "not-a-boolean"
    }

    with pytest.raises(ValidationError):
        LaunchAPI(**invalid_data)