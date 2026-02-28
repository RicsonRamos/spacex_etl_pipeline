from typing import Dict, List, Type
from sqlalchemy import Column, Integer, String, Boolean, DateTime, JSON, BigInteger
from sqlalchemy.orm import DeclarativeMeta
from src.db.base import Base
from src.application.entity_schema import SCHEMAS

# Mapeamento de tipos Python -> SQLAlchemy
TYPE_MAPPING = {
    str: String,
    int: BigInteger,
    bool: Boolean,
    float: BigInteger,   # ajustar se quiser Float
    dict: JSON,
    list: JSON,
}

def create_model(entity_name: str, schema: List[str], pk: str = None) -> Type[DeclarativeMeta]:
    """
    Gera dinamicamente um model SQLAlchemy a partir de uma lista de colunas.

    Args:
        entity_name: Nome lógico da entidade (rockets, launches)
        schema: Lista de nomes de colunas
        pk: Nome da coluna primária (opcional)

    Returns:
        Classe do model SQLAlchemy
    """
    attrs = {"__tablename__": entity_name, "__table_args__": {"extend_existing": True}}

    for col in schema:
        # Toda coluna será tipo JSON para dados extras, exceto algumas específicas
        # Aqui você pode customizar tipo por coluna se quiser
        col_type = JSON

        # Tornar a PK como primária se especificada
        if pk and col == pk:
            attrs[col] = Column(col_type, primary_key=True)
        else:
            attrs[col] = Column(col_type, nullable=True)

    return type(entity_name.capitalize(), (Base,), attrs)

# Exemplo de uso: criar todos os models automaticamente
MODELS: Dict[str, Type[DeclarativeMeta]] = {}
for entity, cols in SCHEMAS.items():
    pk = "rocket_id" if entity == "rockets" else "launch_id"
    model = create_model(entity, cols, pk=pk)
    MODELS[entity] = model