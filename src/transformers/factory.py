from typing import Type
from src.transformers.base import BaseTransformer
from src.transformers.launch import LaunchTransformer
from src.transformers.rocket import RocketTransformer



class TransformerFactory:
    """
    A factory for creating transformers.

    The factory is responsible for managing a registry of available transformers.
    """

    _registry: dict[str, Type[BaseTransformer]] = {}

    @classmethod
    def register(cls, name: str, transformer_cls: Type[BaseTransformer]):
        """
        Registers a transformer in the factory.

        Args:
            name (str): The name of the transformer.
            transformer_cls (Type[BaseTransformer]): The class of the transformer.

        Raises:
            ValueError: If the name is already registered.
        """
        if name in cls._registry:
            raise ValueError(f"Transformer '{name}' already registered.")
        cls._registry[name] = transformer_cls

    @classmethod
    def create(cls, name: str) -> BaseTransformer:
        """
        Creates an instance of a registered transformer.

        Args:
            name (str): The name of the transformer.

        Returns:
            BaseTransformer: An instance of the transformer.

        Raises:
            ValueError: If the transformer is not registered.
        """
        if name not in cls._registry:
            raise ValueError(f"Transformer '{name}' not registered.")
        return cls._registry[name]()


TransformerFactory.register("launches", LaunchTransformer)
TransformerFactory.register("rockets", RocketTransformer)
