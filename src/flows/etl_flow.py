import polars as pl
import structlog
from typing import Optional, List, Tuple
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.cache_policies import NO_CACHE

# Componentes internos
from src.extract.spacex_api import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.loader import PostgresLoader
from src.config.schema_registry import SCHEMA_REGISTRY
from src.utils.dbt_tools import run_dbt
from src.utils.monitoring import start_metrics_server
from src.utils.metrics_service import MetricsService
from src.utils.alert_service import AlertService
from src.config.settings import get_settings

logger = structlog.get_logger()

@task(name="Data Quality Check", tags=["quality"])
def data_quality_task(df: pl.DataFrame, endpoint: str) -> bool:
    """Valida a integridade dos dados antes da carga Silver."""
    from src.utils.data_quality import validate_schema, check_nulls
    
    if df.is_empty():
        logger.warning("Dataframe vazio para validação", endpoint=endpoint)
        return False
        
    validate_schema(df, endpoint)
    check_nulls(df, df.columns)
    
    logger.info("Qualidade de dados aprovada", endpoint=endpoint)
    return True

@task(
    retries=3,
    retry_delay_seconds=30,
    name="Process SpaceX Entity",
    tags=["ingestion"],
    cache_policy=NO_CACHE
)
def process_entity_task(
    endpoint: str,
    loader: PostgresLoader,
    transformer: SpaceXTransformer,
    extractor: SpaceXExtractor,
    metrics: MetricsService,
    alerts: AlertService,
    incremental: bool = False
) -> int:
    """Executa o ciclo Bronze -> Silver para uma entidade específica."""
    try:
        logger.info("Iniciando processamento de entidade", endpoint=endpoint)
        
        # 1. Extração
        raw_data = extractor.fetch(endpoint)
        if not raw_data:
            logger.info("Nenhum dado retornado pela API", endpoint=endpoint)
            return 0

        # 2. Carga Bronze (Raw)
        df_raw = pl.DataFrame(raw_data)
        loader.load_bronze(df_raw, table=f"bronze_{endpoint}")

        # 3. Transformação
        df_silver = transformer.transform(endpoint, raw_data)
        
        # 4. Data Quality (Execução síncrona dentro da task de processamento)
        is_valid = data_quality_task.fn(df_silver, endpoint)
        
        if not is_valid:
            raise ValueError(f"Falha na qualidade dos dados para {endpoint}")

        # 5. Carga Silver (Upsert/Merge)
        rows_affected = loader.load_silver(df_silver, entity=endpoint, incremental=incremental)
        
        # Métricas e Logs
        metrics.record_loaded(endpoint, rows_affected)
        logger.info("Entidade processada com sucesso", endpoint=endpoint, rows=rows_affected)
        
        return rows_affected

    except Exception as e:
        error_msg = f"Erro crítico na entidade {endpoint}: {str(e)}"
        logger.error(error_msg)
        alerts.alert(error_msg)
        metrics.record_failure(endpoint)
        raise  # Garante que o Prefect registre a task como 'Failed'

@flow(
    name="SpaceX Enterprise ETL",
    task_runner=ConcurrentTaskRunner(),
    description="Pipeline Medallion SpaceX - Arquitetura robusta para produção"
)
def spacex_main_pipeline(
    incremental: bool = False,
    run_dbt_flag: bool = True,
    batch_size: int = 1000,
    connection_string: Optional[str] = None # CORRIGIDO: Opcional para evitar erro de validação
):
    # Carrega configurações centralizadas
    settings = get_settings()
    
    # Prioriza parâmetro de entrada, caso contrário usa DATABASE_URL do .env via Pydantic
    db_url = connection_string or settings.DATABASE_URL
    
    if not db_url:
        raise RuntimeError("DATABASE_URL não configurada no ambiente nem passada via parâmetro.")

    # Inicialização de serviços
    loader = PostgresLoader(connection_string=db_url)
    transformer = SpaceXTransformer()
    extractor = SpaceXExtractor()
    metrics = MetricsService()
    alerts = AlertService()

    # Exposição de métricas para Prometheus
    try:
        start_metrics_server(8000)
    except Exception:
        logger.warning("Servidor de métricas já está rodando ou porta ocupada.")

    # Orquestração das Tasks em paralelo
    futures = []
    for endpoint in SCHEMA_REGISTRY.keys():
        future = process_entity_task.submit(
            endpoint=endpoint,
            loader=loader,
            transformer=transformer,
            extractor=extractor,
            metrics=metrics,
            alerts=alerts,
            incremental=incremental
        )
        futures.append(future)

    # Coleta de resultados e monitoramento
    total_rows = 0
    for future in futures:
        try:
            # Espera a conclusão de cada task e soma registros
            rows = future.result()
            total_rows += (rows or 0)
        except Exception:
            # Se uma task falhar, o erro já foi logado nela. O flow continua para as demais.
            continue

    logger.info("Ciclo de ingestão finalizado", total_ingested=total_rows)

    # Execução do dbt para modelagem Gold/Analytics
    if run_dbt_flag:
        logger.info("Iniciando transformações dbt (Gold Layer)")
        run_dbt()

if __name__ == "__main__":
    # Execução manual ou via cron/docker
    spacex_main_pipeline()