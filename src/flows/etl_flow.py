import structlog
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

from src.extract.spacex_api import SpaceXExtractor
from src.extract.transformer import SpaceXTransformer  # Ajustado path
from src.load.postgres_loader import PostgresLoader     # Ajustado path
from src.utils.monitoring import (
    EXTRACT_COUNT, 
    SILVER_COUNT, 
    start_metrics_server, 
    slack_notify
)
from src.utils.dbt_tools import run_dbt

logger = structlog.get_logger()

@task(
    retries=3, 
    retry_delay_seconds=30, 
    name="Process SpaceX Entity",
    tags=["spacex-ingestion"]
)
def process_entity_task(endpoint: str):
    extractor = SpaceXExtractor()
    transformer = SpaceXTransformer()
    loader = PostgresLoader()

    try:
        # 0. PRE-FLIGHT CHECK
        loader.validate_and_align_schema(endpoint)

        # 1. WATERMARK (Marca d'água para carga incremental)
        # O Registry define a tabela silver correta para cada entidade
        from src.config.schema_registry import SCHEMA_REGISTRY
        schema_cfg = SCHEMA_REGISTRY.get(endpoint)
        last_date = loader.get_last_ingested(schema_cfg.silver_table)

        # 2. EXTRAÇÃO (Focada em Contrato e Volume)
        raw_data = extractor.fetch(endpoint) 
        EXTRACT_COUNT.labels(endpoint).inc(len(raw_data))

        if not raw_data:
            logger.info("API retornou dataset vazio", endpoint=endpoint)
            return

        # 3. CAMADA BRONZE (Audit Trail)
        loader.load_bronze(raw_data, endpoint, source="spacex_api_v5")

        # 4. TRANSFORMAÇÃO (Filtro incremental vetorizado com Polars)
        df = transformer.transform(endpoint, raw_data, last_ingested=last_date)
        
        if df.is_empty():
            logger.info("Nenhum registro novo detectado após transformação", endpoint=endpoint)
            return

        # 5. CAMADA SILVER (Upsert/Sincronização)
        rows_upserted = loader.upsert_silver(df, endpoint)
        SILVER_COUNT.labels(endpoint).inc(rows_upserted)

    except Exception as e:
        logger.error("Falha na task de entidade", endpoint=endpoint, error=str(e))
        slack_notify(f"❌ Falha crítica no pipeline SpaceX: Entidade '{endpoint}'")
        raise

@flow(
    name="SpaceX Enterprise ETL",
    task_runner=ConcurrentTaskRunner(),
    description="Pipeline Medallion para extração e modelagem de dados da SpaceX"
)
def spacex_main_pipeline():
    # Inicializa servidor de métricas Prometheus
    start_metrics_server(8000)
    
    # Entidades registradas no SCHEMA_REGISTRY
    entities = ["rockets", "launches"]

    # Execução Paralela: Otimiza uso de I/O de rede e CPU
    logger.info("Iniciando processamento paralelo de entidades")
    futures = [process_entity_task.submit(entity) for entity in entities]

    # BARREIRA DE SINCRONIZAÇÃO
    # Aguarda todas as cargas Silver terminarem antes de iniciar a camada Gold
    for f in futures:
        f.wait()

    # 6. CAMADA GOLD (dbt)
    # Transforma tabelas Silver em Fatos e Dimensões
    logger.info("Iniciando transformações analíticas (dbt)")
    run_dbt()

if __name__ == "__main__":
    spacex_main_pipeline()
