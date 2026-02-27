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

EXPECTED_COLUMNS = {
    "rockets": ["id", "name", "type", "first_flight"],
    "launches": ["id", "rocket_id", "launch_date", "success"],
}

class ETLProcessor:
    def __init__(self, entity: str, real_api: bool, incremental: bool):
        self.entity = entity
        self.real_api = real_api
        self.incremental = incremental
        self.extractor = get_extractor(self.entity)()
        self.transformer = TransformerFactory.create(self.entity)
        self.bronze_loader = BronzeLoader()
        self.silver_loader = SilverLoader()
        self.watermark_manager = WatermarkManager()
        self.schema_validator = SchemaValidator()

    def validate_schema(self):
        expected_cols = EXPECTED_COLUMNS.get(self.entity, [])
        self.schema_validator.validate_table_columns(self.entity, expected_columns=expected_cols)

    def get_last_ingested(self):
        last_date = None
        if self.incremental:
            last_date = self.watermark_manager.get_last_ingested(self.entity)
            if last_date and last_date.tzinfo is None:
                last_date = last_date.replace(tzinfo=timezone.utc)
        return last_date

    def extract_data(self):
        return self.extractor.extract(real_api=self.real_api)

    def transform_data(self, raw_data, last_date):
        return self.transformer.transform(raw_data, last_ingested=last_date)

    def load_bronze(self, raw_data):
        self.bronze_loader.load(raw_data, entity=self.entity, source="spacex_api_v5")

    def load_silver(self, df_silver):
        rows_upserted = self.silver_loader.upsert(df_silver, entity=self.entity)
        SILVER_COUNT.labels(self.entity).inc(rows_upserted)
        return rows_upserted

    def run_etl(self):
        try:
            self.validate_schema()

            last_date = self.get_last_ingested()

            # Extração
            raw_data = self.extract_data()
            if not raw_data:
                logger.info("API retornou dataset vazio", entity=self.entity)
                return 0
            EXTRACT_COUNT.labels(self.entity).inc(len(raw_data))

            # Persistência Bronze
            self.load_bronze(raw_data)

            # Transformação Silver
            df_silver = self.transform_data(raw_data, last_date)
            if df_silver.empty:
                logger.info("Nenhum registro novo após transformação", entity=self.entity)
                return 0

            # Carregamento Silver
            rows_upserted = self.load_silver(df_silver)

            logger.info("ETL concluído para entidade", entity=self.entity, rows_processed=rows_upserted)
            return rows_upserted

        except Exception as e:
            logger.error("Falha na task de entidade", entity=self.entity, error=str(e))
            slack_notify(f"Falha crítica no pipeline SpaceX: Entidade '{self.entity}'")
            raise


@task(retries=3, retry_delay_seconds=30, name="Process SpaceX Entity")
def process_entity_task(entity: str, real_api: bool = False, incremental: bool = False):
    processor = ETLProcessor(entity, real_api, incremental)
    return processor.run_etl()