from typing import Dict, Type
from src.transformers.base import BaseTransformer
from src.transformers.launch import LaunchTransformer
from src.transformers.rocket import RocketTransformer
from src.application.entity_schema import SCHEMAS

class TransformerFactory:
    _registry: Dict[str, Type[BaseTransformer]] = {
        "launches": LaunchTransformer,
        "rockets": RocketTransformer
    }

    @classmethod
    def create(cls, name: str) -> BaseTransformer:
        transformer_cls = cls._registry.get(name)
        if not transformer_cls:
            raise ValueError(f"Transformer for entity '{name}' not found in registry.")
        
        # Injeta o schema da Silver definido em SCHEMAS[name]
        return transformer_cls(columns_schema=SCHEMAS[name])
