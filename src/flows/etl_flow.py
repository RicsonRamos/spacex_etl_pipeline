import polars as pl
from typing import Optional, Any

from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.task_runners import ConcurrentTaskRunner
import structlog

from src.extract.spacex_api import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.loader import PostgresLoader
from src.config.schema_registry import SCHEMA_REGISTRY
from src.utils.dbt_tools import run_dbt
from src.flows.data_quality import validate_schema, check_nulls
from src.utils.metrics_service import MetricsService
from src.utils.alert_service import AlertService
from src.config.settings import get_settings

logger = structlog.get_logger()



# TASK: DATA QUALITY


@task(name="Data Quality Check")
def data_quality_task(df: pl.DataFrame, endpoint: str):
    validate_schema(df, df.columns)
    check_nulls(df, df.columns)
    logger.info("Data quality passed", endpoint=endpoint)
    return True



# TASK: PROCESS ENTITY


@task(
    retries=2,
    retry_delay_seconds=5,
    name="Process SpaceX Entity",
    tags=["spacex-ingestion"],
    cache_policy=NO_CACHE
)
def process_entity_task(
    endpoint: str,
    loader: PostgresLoader,
    transformer: SpaceXTransformer,
    extractor: SpaceXExtractor,
    metrics: MetricsService,
    alerts: AlertService,
    batch_size: int = 1000,
    incremental: bool = False,
):
    try:
        schema_cfg = SCHEMA_REGISTRY.get(endpoint)
        if not schema_cfg:
            logger.warning("Schema não encontrado para endpoint", endpoint=endpoint)
            return 0

        # 🔹 Watermark opcional
        last_date = loader.get_last_ingested(schema_cfg.silver_table) if hasattr(loader, "get_last_ingested") else None

        # 🔹 Extract
        raw_data = extractor.fetch(endpoint)
        if not raw_data:
            logger.info("No data returned from API", endpoint=endpoint)
            return 0

        # 🔹 Bronze
        df_bronze = pl.DataFrame(raw_data)
        loader.load_bronze(df_bronze, table=f"bronze_{endpoint}")

        # 🔹 Transform
        df_silver = transformer.transform(endpoint, raw_data)
        if df_silver.is_empty():
            logger.info("No new rows after transform", endpoint=endpoint)
            return 0

        # 🔹 Data Quality
        dq_task = data_quality_task.submit(df_silver, endpoint)
        dq_task.result()  # espera o fim da validação

        # 🔹 Upsert Silver
        rows = loader.load_silver(df_silver, entity=endpoint)
        metrics.record_loaded(endpoint, rows)

        return rows

    except Exception as e:
        logger.error("ETL failed", endpoint=endpoint, error=str(e))
        alerts.alert(f"Falha crítica no pipeline SpaceX: {endpoint}")
        metrics.record_failure(endpoint)
        raise



# FLOW PRINCIPAL MULTI-ENDPOINT


@flow(
    name="SpaceX Enterprise ETL",
    task_runner=ConcurrentTaskRunner(),
)
def spacex_main_pipeline(
    batch_size: int = 1000,
    connection_string: Optional[str] = None,
    loader: Optional[Any] = None,
    transformer: Optional[Any] = None,
    extractor: Optional[Any] = None,
    metrics: Optional[Any] = None,
    alerts: Optional[Any] = None,
    run_dbt_flag: bool = True,
    incremental: bool = False,
) -> None:

    settings = get_settings()
    connection_string = connection_string or settings.DATABASE_URL

    # Instâncias com fallback
    loader = loader or PostgresLoader(connection_string=connection_string)
    transformer = transformer or SpaceXTransformer()
    extractor = extractor or SpaceXExtractor()
    metrics = metrics or MetricsService()
    alerts = alerts or AlertService()

    # Inicia servidor de métricas
    metrics.start_server()
    logger.info("Metrics server iniciado")

    # Logging do início do pipeline
    logger.info(
        "Starting SpaceX ETL pipeline",
        connection_host=connection_string.split("@")[-1]
    )

    # Processa todos os endpoints em paralelo
    task_results = []
    for endpoint in SCHEMA_REGISTRY.keys():
        task_result = process_entity_task.submit(
            endpoint=endpoint,
            loader=loader,
            transformer=transformer,
            extractor=extractor,
            metrics=metrics,
            alerts=alerts,
            batch_size=batch_size,
            incremental=incremental,
        )
        task_results.append((endpoint, task_result))

    # Coleta resultados e atualiza métricas/alertas
    total_rows = 0
    for endpoint, task_result in task_results:
        rows_processed = task_result.result()
        total_rows += rows_processed or 0
        metrics.SILVER_COUNT.labels(endpoint=endpoint).inc(rows_processed or 0)
        logger.info(f"Metrics updated for {endpoint}", processed=rows_processed)
        if (rows_processed or 0) == 0:
            alerts.slack_notify(f"Pipeline SpaceX não processou nenhum registro no endpoint {endpoint}.")
            logger.warning(f"No records processed for {endpoint}, Slack notification sent")

    logger.info("ETL pipeline completed", total_rows=total_rows)

    # dbt (opcional)
    if run_dbt_flag:
        logger.info("dbt run flag enabled - você pode chamar seu dbt runner aqui")


if __name__ == "__main__":
    spacex_main_pipeline(batch_size=1000)