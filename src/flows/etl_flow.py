import structlog
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from datetime import timezone
import pandas as pd

from src.extract.spacex_api import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer  
from src.load.loader import PostgresLoader 
from src.utils.monitoring import EXTRACT_COUNT, SILVER_COUNT, start_metrics_server, slack_notify
from src.utils.dbt_tools import run_dbt
from src.etl.data_quality import validate_schema, check_nulls, check_duplicates, check_date_ranges
from src.config.schema_registry import SCHEMA_REGISTRY

logger = structlog.get_logger()

# Data Quality Task
@task(name="Data Quality Check")
def data_quality_task(df: pd.DataFrame, endpoint: str):
    """Valida dados Silver antes do upsert"""
    validate_schema(df, ["flight_number", "mission_name", "launch_date"])
    check_nulls(df, ["mission_name", "launch_date"])
    check_duplicates(df, ["flight_number"])
    check_date_ranges(df, "launch_date", "2006-03-24", pd.Timestamp.today())
    logger.info("Data quality passed", endpoint=endpoint)
    return True

# ETL Task por entidade
@task(
    retries=3, 
    retry_delay_seconds=30, 
    name="Process SpaceX Entity",
    tags=["spacex-ingestion"]
)
def process_entity_task(endpoint: str):
    """ETL completo de uma entidade SpaceX (Bronze → Transform → Silver)"""
    
    extractor = SpaceXExtractor()
    transformer = SpaceXTransformer()
    loader = PostgresLoader()

    try:
        
        # Pre-flight: valida schema Bronze/Silver
        
        loader.validate_and_align_schema(endpoint)

        # Marca d'água para carga incremental
        schema_cfg = SCHEMA_REGISTRY.get(endpoint)
        last_date = loader.get_last_ingested(schema_cfg.silver_table)
        if last_date.tzinfo is None:
            last_date = last_date.replace(tzinfo=timezone.utc)

        
        # Extração de dados via API
        
        raw_data = extractor.fetch(endpoint)
        EXTRACT_COUNT.labels(endpoint).inc(len(raw_data))
        if not raw_data:
            logger.info("API retornou dataset vazio", endpoint=endpoint)
            return 0

        # Persistência Bronze (raw JSONB + UTC)
        loader.load_bronze(raw_data, entity=endpoint, source="spacex_api_v5")

        
        # Transformação Silver (UTC-aware)
        
        df_silver = transformer.transform(endpoint, raw_data, last_ingested=last_date)
        if df_silver.is_empty():
            logger.info("Nenhum registro novo após transformação", endpoint=endpoint)
            return 0

        
        # Data Quality antes do Upsert
        
        data_quality_task.submit(df_silver, endpoint)

        
        # Upsert Silver
        
        rows_upserted = loader.upsert_silver(df_silver, entity=endpoint)
        SILVER_COUNT.labels(endpoint).inc(rows_upserted)

        logger.info(
            "ETL concluído para entidade",
            endpoint=endpoint,
            rows_processed=rows_upserted
        )
        return rows_upserted

    except Exception as e:
        logger.error("Falha na task de entidade", endpoint=endpoint, error=str(e))
        slack_notify(f"Falha crítica no pipeline SpaceX: Entidade '{endpoint}'")
        raise

# Flow principal
@flow(
    name="SpaceX Enterprise ETL",
    task_runner=ConcurrentTaskRunner(),
    description="Pipeline Medallion para extração e modelagem de dados da SpaceX"
)
def spacex_main_pipeline(incremental: bool = False):
    """Fluxo principal ETL SpaceX (Rockets → Launches → Gold/dbt)"""
    
    # Inicializa métricas Prometheus
    start_metrics_server(8000)
    
    
    # Ingestão de dimensões base (Rockets)
    
    logger.info("Iniciando ingestão de Rockets")
    rocket_future = process_entity_task.submit("rockets")
    rocket_future.wait()  # garante que FKs existam antes de launches

    
    # Ingestão de fatos (Launches)
    
    logger.info("Iniciando ingestão de Launches")
    launch_future = process_entity_task.submit("launches")
    launch_future.wait()

    
    # Camada Gold (transformações analíticas via dbt)
    
    logger.info("Iniciando transformações analíticas (dbt)")
    run_dbt()

# Entry point
if __name__ == "__main__":
    spacex_main_pipeline()
