from typing import Dict, Type

from src.application.etl_service import ETLService
from src.application.entity_config import EntityConfig

from src.extract.rockets import RocketExtract
from src.extract.launches import LaunchExtract

from src.transformers.rocket import RocketTransformer
from src.transformers.launch import LaunchTransformer

from src.loaders.bronze_loader import BronzeLoader
from src.loaders.silver_loader import SilverLoader
from src.loaders.schema_validator import SchemaValidator
from src.loaders.watermark import WatermarkManager

from src.utils.monitoring.metrics import PipelineMetrics
from src.utils.monitoring.notifier import SlackNotifier


class ETLFactory:
    """
    Factory responsável por criar instâncias do ETLService
    seguindo princípios SOLID:

    - SRP: Apenas instancia dependências
    - OCP: Novas entidades podem ser registradas
    - DIP: ETLService depende de abstrações
    """

    _registry: Dict[str, Dict[str, Type]] = {
        "rockets": {
            "extractor": RocketExtract,
            "transformer": RocketTransformer,
        },
        "launches": {
            "extractor": LaunchExtract,
            "transformer": LaunchTransformer,
        },
    }

    @classmethod
    def create(
        cls,
        entity: str,
        incremental: bool,
        real_api: bool,
    ) -> ETLService:
        """
        Cria instância do ETLService corretamente configurada.

        Args:
            entity: Nome lógico da entidade (ex: "rockets")
            incremental: Executar modo incremental
            real_api: Usar API real ou mock

        Returns:
            ETLService configurado
        """

        if entity not in cls._registry:
            raise ValueError(
                f"Entity '{entity}' not supported. "
                f"Available entities: {list(cls._registry.keys())}"
            )

        # Configuração centralizada da entidade
        config = EntityConfig(name=entity)

        # Instanciar componentes específicos
        extractor = cls._registry[entity]["extractor"]()
        transformer = cls._registry[entity]["transformer"]()

        # Infraestrutura desacoplada do nome físico
        bronze_loader = BronzeLoader(table_name=config.bronze_table)
        silver_loader = SilverLoader(table_name=config.silver_table)
        watermark_manager = WatermarkManager(config.silver_table)

        # Componentes compartilhados
        schema_validator = SchemaValidator()
        metrics = PipelineMetrics()
        notifier = SlackNotifier(webhook_url=None)

        return ETLService(
            entity=config.name,
            incremental=incremental,
            real_api=real_api,
            extractor=extractor,
            transformer=transformer,
            bronze_loader=bronze_loader,
            silver_loader=silver_loader,
            watermark=watermark_manager,
            metrics=metrics,
            notifier=notifier,
            schema_validator=schema_validator,
        )

    @classmethod
    def register_entity(
        cls,
        entity: str,
        extractor_class: Type,
        transformer_class: Type,
    ) -> None:
        """
        Permite registrar nova entidade sem modificar código existente.
        (Open/Closed Principle)
        """
        cls._registry[entity] = {
            "extractor": extractor_class,
            "transformer": transformer_class,
        }