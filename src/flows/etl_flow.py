import polars as pl
import structlog
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.cache_policies import NO_CACHE

from src.extract.spacex_api import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.loader import PostgresLoader
from src.config.schema_registry import SCHEMA_REGISTRY
from src.utils.dbt_tools import run_dbt
from src.utils.monitoring import EXTRACT_COUNT, SILVER_COUNT, start_metrics_server, slack_notify
from src.utils.metrics_service import MetricsService
from src.utils.alert_service import AlertService
from src.config.settings import get_settings

logger = structlog.get_logger()

# Task de Qualidade de Dados
@task(name="Data Quality Check")
def data_quality_task(df: pl.DataFrame, endpoint: str):
    from src.utils.data_quality import validate_schema, check_nulls
    
    # Validação de Schema e Nulos
    validate_schema(df, df.columns)
    check_nulls(df, df.columns)
    
    logger.info("Data quality passed", endpoint=endpoint)
    return True

# Task de Processamento da Entidade
@task(
    retries=3,
    retry_delay_seconds=30,
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
    incremental: bool = False,
):
    try:
        # Verificação de Schema no SCHEMA_REGISTRY
        schema_cfg = SCHEMA_REGISTRY.get(endpoint)
        if not schema_cfg:
            logger.warning("Schema não encontrado para endpoint", endpoint=endpoint)
            return 0

        # Watermark: Última data processada para carga incremental
        last_date = loader.get_last_ingested(schema_cfg.silver_table) if hasattr(loader, "get_last_ingested") else None

        # Extração de Dados
        raw_data = extractor.fetch(endpoint)
        EXTRACT_COUNT.labels(endpoint).inc(len(raw_data))
        
        if not raw_data:
            logger.info("API retornou dataset vazio", endpoint=endpoint)
            return 0

        # Carga Bronze (raw JSONB + UTC)
        df_bronze = pl.DataFrame(raw_data)
        loader.load_bronze(df_bronze, table=f"bronze_{endpoint}")

        # Transformação para Silver
        df_silver = transformer.transform(endpoint, raw_data)
        if df_silver.is_empty():
            logger.info("Nenhum registro novo após transformação", endpoint=endpoint)
            return 0

        # Qualidade de Dados: Valida antes de carregar para Silver
        dq_task = data_quality_task.submit(df_silver, endpoint)
        dq_task.result()  # Espera pela validação de qualidade de dados

        # Carga Silver (Upsert)
        rows_upserted = loader.load_silver(df_silver, entity=endpoint)
        SILVER_COUNT.labels(endpoint).inc(rows_upserted)

        # Registra as métricas
        metrics.record_loaded(endpoint, rows_upserted)

        logger.info("ETL concluído para entidade", endpoint=endpoint, rows_processed=rows_upserted)
        return rows_upserted

    except Exception as e:
        logger.error("Falha na task de entidade", endpoint=endpoint, error=str(e))
        alerts.alert(f"Falha crítica no pipeline SpaceX: Entidade '{endpoint}'")
        metrics.record_failure(endpoint)
        raise

# Flow Principal com Processamento Paralelizado
@flow(
    name="SpaceX Enterprise ETL",
    task_runner=ConcurrentTaskRunner(),
    description="Pipeline Medallion para extração e modelagem de dados da SpaceX"
)
def spacex_main_pipeline(
    incremental: bool = False,
    run_dbt_flag: bool = True,
    batch_size: int = 1000,
    connection_string: str = None
):
    settings = get_settings()
    
    # Se o connection_string não for passado, pega das configurações
    connection_string = connection_string or settings.DATABASE_URL
    
    if not connection_string:
        raise ValueError("O connection_string é obrigatório e não foi fornecido nem nas configurações")

    # Instâncias de componentes com fallback
    loader = PostgresLoader(connection_string=connection_string)
    transformer = SpaceXTransformer()
    extractor = SpaceXExtractor()
    metrics = MetricsService()
    alerts = AlertService()

    # Inicia servidor de métricas Prometheus
    start_metrics_server(8000)
    logger.info("Metrics server iniciado")

    # Logging do início do pipeline
    logger.info(
        "Iniciando pipeline SpaceX ETL",
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
            incremental=incremental
        )
        task_results.append((endpoint, task_result))

    # Coleta resultados e atualiza métricas/alertas
    total_rows = 0
    for endpoint, task_result in task_results:
        rows_processed = task_result.result()
        total_rows += rows_processed or 0
        metrics.SILVER_COUNT.labels(endpoint=endpoint).inc(rows_processed or 0)
        logger.info(f"Métricas atualizadas para {endpoint}", processed=rows_processed)
        if (rows_processed or 0) == 0:
            alerts.slack_notify(f"Pipeline SpaceX não processou nenhum registro no endpoint {endpoint}.")
            logger.warning(f"Sem registros processados para {endpoint}, notificação enviada ao Slack")

    logger.info("Pipeline ETL concluído", total_rows=total_rows)

    # dbt (opcional)
    if run_dbt_flag:
        logger.info("dbt run flag enabled - você pode chamar seu dbt runner aqui")
        run_dbt()

if __name__ == "__main__":
    spacex_main_pipeline(batch_size=1000)