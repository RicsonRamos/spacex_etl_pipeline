import structlog
from prefect import flow, task
from src.extract.spacex_api import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.loader import PostgresLoader
from src.utils.monitoring import EXTRACT_COUNT, SILVER_COUNT, start_metrics_server
from src.utils.dbt_tools import run_dbt

logger = structlog.get_logger()

@task(name="Process Entity Task")
def process_entity(endpoint: str):
    """Encapsula o ciclo de vida de uma entidade (Ex: launches)."""
    
    # 1. Inicialização de componentes
    extractor = SpaceXExtractor()
    transformer = SpaceXTransformer()
    loader = PostgresLoader()

    # 2. Extração Incremental
    # Buscamos a última data na tabela silver correspondente
    last_date = loader.get_last_ingested(f"silver_{endpoint}")
    raw_data = extractor.fetch(endpoint, incremental=True, last_ingested=last_date)
    EXTRACT_COUNT.labels(endpoint).inc(len(raw_data))

    if not raw_data:
        logger.info(f"Sem novos dados para {endpoint}")
        return

    # 3. Bronze (Raw Load)
    loader.load_bronze(raw_data, endpoint, source="spacex_api")

    # 4. Transformação (Polars)
    df = transformer.transform(endpoint, raw_data)

    # 5. Silver (Upsert)
    rows = loader.upsert_silver(df, endpoint)
    SILVER_COUNT.labels(endpoint).inc(rows)

@flow(name="SpaceX Enterprise Pipeline")
def spacex_main_flow():
    # Setup de monitoramento
    start_metrics_server(8000)
    
    entities = ["rockets", "launches", "launchpads"]

    # Execução das Tasks (O Prefect lida com a concorrência se configurado)
    for entity in entities:
        process_entity(entity)

    # Camada Gold (Transformações Pesadas SQL)
    run_dbt()

if __name__ == "__main__":
    spacex_main_flow()
