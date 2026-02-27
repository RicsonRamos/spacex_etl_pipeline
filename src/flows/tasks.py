# src/flows/tasks.py
from datetime import timezone
import structlog
from prefect import task

from src.extract.factory import get_extractor
from src.transformers.factory import TransformerFactory
from src.loaders.bronze_loader import BronzeLoader
from src.loaders.silver_loader import SilverLoader
from src.loaders.watermark import WatermarkManager
from src.loaders.schema_validator import SchemaValidator
from src.utils.monitoring import EXTRACT_COUNT, SILVER_COUNT, slack_notify

logger = structlog.get_logger()

# Mapa de colunas esperadas por entidade
EXPECTED_COLUMNS = {
    "rockets": ["id", "name", "type", "first_flight"],
    "launches": ["id", "rocket_id", "launch_date", "success"],
    # adicionar outras entidades se houver
}


@task(retries=3, retry_delay_seconds=30, name="Process SpaceX Entity")
def process_entity_task(entity: str, real_api: bool = False, incremental: bool = False):
    """
    ETL completo de uma entidade SpaceX (Bronze → Transform → Silver)
    """

    # Descobre extractor e transformer
    extractor_cls = get_extractor(entity)
    extractor = extractor_cls()  # agora sempre é classe, cria instância
    transformer = TransformerFactory.create(entity)

    bronze_loader = BronzeLoader()
    silver_loader = SilverLoader()
    watermark_manager = WatermarkManager()
    schema_validator = SchemaValidator()

    try:
        # Valida schema e colunas
        expected_cols = EXPECTED_COLUMNS.get(entity, [])
        schema_validator.validate_table_columns(entity, expected_columns=expected_cols)

        # Marca d'água para incremental
        last_date = None
        if incremental:
            last_date = watermark_manager.get_last_ingested(entity)
            if last_date and last_date.tzinfo is None:
                last_date = last_date.replace(tzinfo=timezone.utc)

        # Extração
        raw_data = extractor.extract(real_api=real_api)
        if not raw_data:
            logger.info("API retornou dataset vazio", entity=entity)
            return 0

        EXTRACT_COUNT.labels(entity).inc(len(raw_data))

        # Persistência Bronze
        bronze_loader.load(raw_data, entity=entity, source="spacex_api_v5")

        # Transformação Silver
        df_silver = transformer.transform(raw_data, last_ingested=last_date)
        if df_silver.empty:
            logger.info("Nenhum registro novo após transformação", entity=entity)
            return 0

        # Upsert Silver
        rows_upserted = silver_loader.upsert(df_silver, entity=entity)
        SILVER_COUNT.labels(entity).inc(rows_upserted)

        logger.info(
            "ETL concluído para entidade",
            entity=entity,
            rows_processed=rows_upserted,
        )
        return rows_upserted

    except Exception as e:
        logger.error("Falha na task de entidade", entity=entity, error=str(e))
        slack_notify(f"Falha crítica no pipeline SpaceX: Entidade '{entity}'")
        raise