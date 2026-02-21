import pytest
from pydantic import ValidationError
from src.extract.schemas import RocketSchema

def test_rocket_schema_valid_data():
    """Valida se o mapeamento de alias (id -> rocket_id) está funcionando."""
    payload = {
        "id": "5e9d0d95eda69955f709d1eb",
        "name": "Falcon 9",
        "type": "rocket",
        "active": True
    }
    rocket = RocketSchema(**payload)
    assert rocket.rocket_id == "5e9d0d95eda69955f709d1eb"

def test_rocket_schema_invalid_data():
    """Garante que dados faltantes lancem erro (Rigor de Tipagem)."""
    incomplete_payload = {"name": "Falcon 9"}  # Falta 'id' e 'type'
    with pytest.raises(ValidationError):
        RocketSchema(**incomplete_payload)