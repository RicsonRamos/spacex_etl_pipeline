import structlog
from prefect import flow, task
from src.extract.spacex_api import spacex_client
from src.transform.transformer import transformer
from src.load.postgres_loader import loader

logger = structlog.get_logger()

@task(retries=3, retry_delay_seconds=10)
def process_endpoint(endpoint: str, table_name: str, pk_col: str):
    """
    Task atômica: Extrai, Transforma e Carrega um domínio específico.
    """
    log = logger.bind(endpoint=endpoint, table=table_name)
    log.info(f"Processando domínio: {endpoint}")

    # 1. Extract
    raw_data = spacex_client.fetch_data(endpoint)
    
    # 2. Transform (Seleciona o método dinamicamente)
    transform_method = getattr(transformer, f"transform_{endpoint}")
    df_clean = transform_method(raw_data)
    
    # 3. Load
    loader.upsert_dataframe(df_clean, table_name, pk_col)
    
    log.info(f"Finalizado com sucesso: {endpoint}")

@flow(name="SpaceX ETL Production Pipeline")
def spacex_etl_flow():
    """
    Flow principal que define a ordem de execução e dependências.
    """
    logger.info("Iniciando Flow de Produção SpaceX")

    # Ordem importa para Integridade Referencial (FKs)
    # Primeiro as dimensões (Rockets, Launchpads)
    process_endpoint("rockets", "rockets", "rocket_id")
    process_endpoint("launchpads", "launchpads", "launchpad_id")
    
    # Depois os fatos e dependentes (Payloads, Launches)
    process_endpoint("payloads", "payloads", "payload_id")
    process_endpoint("launches", "launches", "launch_id")

    logger.info("Pipeline SpaceX finalizado com sucesso")

if __name__ == "__main__":
    spacex_etl_flow()