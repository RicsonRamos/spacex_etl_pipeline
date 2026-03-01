from typing import Dict, Type
from src.transformers.base import BaseTransformer
from src.application.entity_schema import SCHEMAS

class TransformerFactory:
    _registry: Dict[str, Type[BaseTransformer]] = {}

    @classmethod
    def register(cls, name: str, transformer_cls: Type[BaseTransformer]):
        cls._registry[name] = transformer_cls

    @classmethod
    def create(cls, name: str) -> BaseTransformer:
        transformer_cls = cls._registry.get(name)
        if not transformer_cls:
            raise ValueError(f"Transformer {name} not found.")
        
        # Injeção automática do schema da Silver definido centralizadamente
        return transformer_cls(columns_schema=SCHEMAS[name])

# Registro (Pode ser feito em um arquivo de inicialização de app)
from src.transformers.launch import LaunchTransformer
from src.transformers.rocket import RocketTransformer
TransformerFactory.register("launches", LaunchTransformer)
TransformerFactory.register("rockets", RocketTransformer)
