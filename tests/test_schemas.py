import pytest
from pydantic import ValidationError
from src.extract.schemas import LaunchSchema

def test_launch_schema_validation():
    # Dados válidos
    valid_data = {
        "id": "5eb87cd9ffd86e000604b32a",
        "name": "Falcon 1",
        "date_utc": "2006-03-24T22:30:00.000Z",
        "success": False,
        "rocket": "5e9d0d95eda69955f709d1eb"
    }
    assert LaunchSchema(**valid_data)

def test_launch_schema_invalid_types():
    # Dado inválido (success deveria ser bool, mandamos string incompatível)
    invalid_data = {
        "id": 123, # Deveria ser string
        "name": "Falcon 1",
        "success": "not-a-boolean"
    }
    with pytest.raises(ValidationError):
        LaunchSchema(**invalid_data)
