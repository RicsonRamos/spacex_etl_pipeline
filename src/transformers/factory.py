# src/transformers/factory.py
from typing import Type
from src.transformers.base import BaseTransformer
from src.transformers.launch import LaunchTransformer
from src.transformers.rocket import RocketTransformer


class TransformerFactory:
    """
    Factory para criar instâncias de transformers do ETL.

    Mantém um registro (_registry) de transformers disponíveis
    e permite criar instâncias pelo nome da entidade.
    """

    _registry: dict[str, Type[BaseTransformer]] = {}

    @classmethod
    def register(cls, name: str, transformer_cls: Type[BaseTransformer]):
        """
        Registra um transformer no factory.

        Args:
            name (str): nome da entidade/transformer
            transformer_cls (Type[BaseTransformer]): classe do transformer

        Raises:
            ValueError: se o nome já estiver registrado
        """
        if name in cls._registry:
            raise ValueError(f"Transformer '{name}' já registrado.")
        cls._registry[name] = transformer_cls

    @classmethod
    def get(cls, name: str) -> Type[BaseTransformer]:
        """
        Retorna a classe registrada para um nome de entidade.

        Args:
            name (str): nome da entidade

        Raises:
            ValueError: se o transformer não estiver registrado
        """
        if name not in cls._registry:
            raise ValueError(f"Transformer '{name}' não registrado.")
        return cls._registry[name]

    @classmethod
    def create(cls, name: str) -> BaseTransformer:
        """
        Cria uma instância do transformer registrado para a entidade.

        Args:
            name (str): nome da entidade

        Returns:
            BaseTransformer: instância do transformer

        Raises:
            ValueError: se o transformer não estiver registrado
        """
        transformer_cls = cls.get(name)
        return transformer_cls()


# Registra transformers do projeto
TransformerFactory.register("launches", LaunchTransformer)
TransformerFactory.register("rockets", RocketTransformer)