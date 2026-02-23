from datetime import timezone
import pandas as pd
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prometheus_client import Counter, Summary, start_http_server

from src.extract.spacex_api import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer  
from src.load.loader import PostgresLoader 
from src.utils.monitoring import EXTRACT_COUNT, SILVER_COUNT, slack_notify
from src.utils.dbt_tools import run_dbt
from src.etl.data_quality import validate_schema, check_nulls, check_duplicates, check_date_ranges
from src.config.schema_registry import SCHEMA_REGISTRY
from src.utils.logging import logger  # <-- logging estruturado


# Métricas Prometheus

PROCESS_TIME_SECONDS = Summary("etl_process_time_seconds", "Tempo de execução do ETL", ["endpoint"])
FAILURE_COUNT = Counter("etl_failures_total", "Número de falhas no ETL", ["endpoint"])


# Task: Data Quality

@task(name="Data Quality Check")
def data_quality_task(df: pd.DataFrame, endpoint: str):
    """Valida dados Silver antes do upsert"""
    validate_schema(df, ["flight_number", "mission_name", "launch_date"])
    check_nulls(df, ["mission_name", "launch_date"])
    check_duplicates(df, ["flight_number"])
    check_date_ranges(df, "launch_date", "2006-03-24", pd.Timestamp.today())
    logger.info("Data quality passed", endpoint=endpoint)
    return True


# Task: ETL por entidade

@task(
    retries=3,
    retry_delay_seconds=30,
    name="Process SpaceX Entity",
    tags=["spacex-ingestion"]
)
def process_entity_task(endpoint: str):
    """ETL completo de uma entidade SpaceX (Bronze → Silver)"""
    
    extractor = SpaceXExtractor()
    transformer = SpaceXTransformer()
    loader = PostgresLoader()

    try:
       
        # Pre-flight: valida schema Bronze/Silver
       
        loader.validate_and_align_schema(endpoint)

        schema_cfg = SCHEMA_REGISTRY.get(endpoint)
        last_date = loader.get_last_ingested(schema_cfg.silver_table)
        if last_date.tzinfo is None:
            last_date = last_date.replace(tzinfo=timezone.utc)

       
        # Extração
       
        logger.info("Starting extraction", endpoint=endpoint, last_ingested=str(last_date))
        raw_data = extractor.fetch(endpoint)
        EXTRACT_COUNT.labels(endpoint).inc(len(raw_data))
        logger.info("Extraction completed", endpoint=endpoint, rows=len(raw_data))

        if not raw_data:
            logger.info("API retornou dataset vazio", endpoint=endpoint)
            return 0

       
        # Persistência Bronze
       
        loader.load_bronze(raw_data, entity=endpoint, source="spacex_api_v5")
        logger.info("Bronze load completed", endpoint=endpoint, rows=len(raw_data))

       
        # Transformação Silver + Data Quality
       
        @PROCESS_TIME_SECONDS.labels(endpoint=endpoint).time()
        def transform_and_validate():
            df_silver = transformer.transform(endpoint, raw_data, last_ingested=last_date)
            if df_silver.is_empty():
                logger.info("Nenhum registro novo após transformação", endpoint=endpoint)
                return df_silver
            # Data Quality
            data_quality_task.submit(df_silver, endpoint)
            return df_silver

        df_silver = transform_and_validate()

        if df_silver.is_empty():
            return 0

       
        # Upsert Silver
       
        rows_upserted = loader.upsert_silver(df_silver, entity=endpoint)
        SILVER_COUNT.labels(endpoint).inc(rows_upserted)
        logger.info("Silver upsert completed", endpoint=endpoint, rows_upserted=rows_upserted)

        return rows_upserted

    except Exception as e:
        logger.error("ETL failed", endpoint=endpoint, error=str(e))
        slack_notify(f"Falha crítica no pipeline SpaceX: Entidade '{endpoint}'")
        FAILURE_COUNT.labels(endpoint=endpoint).inc()
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
    start_http_server(8000)
    logger.info("Prometheus metrics server started on port 8000")

   
    # Ingestão de dimensões (Rockets)
   
    logger.info("Starting Rockets ingestion")
    rocket_future = process_entity_task.submit("rockets")
    rocket_future.wait()  # garante que FKs existam antes de launches

   
    # Ingestão de fatos (Launches)
   
    logger.info("Starting Launches ingestion")
    launch_future = process_entity_task.submit("launches")
    launch_future.wait()

   
    # Camada Gold (dbt)
   
    logger.info("Starting analytical transformations (dbt)")
    run_dbt()
    logger.info("ETL pipeline completed successfully")


# Entry point

if __name__ == "__main__":
    spacex_main_pipeline()
