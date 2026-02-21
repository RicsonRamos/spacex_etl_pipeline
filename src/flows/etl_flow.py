import structlog
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

from src.extract.spacex_api import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.loader import PostgresLoader
from src.utils.monitoring import (
    EXTRACT_COUNT, 
    SILVER_COUNT, 
    start_metrics_server, 
    slack_notify
)
from src.utils.dbt_tools import run_dbt

logger = structlog.get_logger()

@task(retries=3, retry_delay_seconds=30, name="Process SpaceX Entity")
def process_entity_task(endpoint: str):
    extractor = SpaceXExtractor()
    transformer = SpaceXTransformer()
    loader = PostgresLoader()

    try:
        # 0. VALIDAÇÃO DE SCHEMA (Pre-flight check)
        # Garante que o banco tem as colunas que o código espera
        loader.validate_and_align_schema(endpoint)

        # 1. ESTADO INCREMENTAL
        table_name = f"silver_{endpoint}"
        last_date = loader.get_last_ingested(table_name)

        # 2. EXTRAÇÃO
        raw_data = extractor.fetch(endpoint, incremental=True, last_ingested=last_date)
        EXTRACT_COUNT.labels(endpoint).inc(len(raw_data))

        if not raw_data:
            logger.info("Sem dados novos", endpoint=endpoint)
            return

        # 3. CARGAS E TRANSFORMAÇÃO
        loader.load_bronze(raw_data, endpoint, source="spacex_api")
        df = transformer.transform(endpoint, raw_data)
        rows_upserted = loader.upsert_silver(df, endpoint)
        
        SILVER_COUNT.labels(endpoint).inc(rows_upserted)
        loader.refresh_gold_view(endpoint)

    except Exception as e:
        logger.error("Falha na entidade", endpoint=endpoint, error=str(e))
        slack_notify(f"❌ Erro: {endpoint} falhou.")
        raise

@flow(
    name="SpaceX Enterprise ETL",
    task_runner=ConcurrentTaskRunner() 
)
def spacex_main_pipeline():
    start_metrics_server(8000)
    entities = ["rockets", "launches", "launchpads"]

    # .submit() garante que as tasks rodem em paralelo
    futures = [process_entity_task.submit(entity) for entity in entities]

    # BLOQUEIO SINCRONO: O DBT só roda após TODAS as tasks terminarem
    # Sem isso, o DBT pode rodar antes de os dados chegarem na Silver
    for f in futures:
        f.wait()

    run_dbt()

if __name__ == "__main__":
    spacex_main_pipeline()
