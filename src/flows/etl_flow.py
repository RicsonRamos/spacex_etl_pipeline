import structlog
from prefect import flow, task, get_run_logger
from prefect.context import get_run_context

from src.extract.spacex_api import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.loader import PostgresLoader


logger = structlog.get_logger()

def get_enriched_logger():
    """
    Returns a logger that is enriched with Prefect context information.

    If Prefect context information is available, the logger is bound with the following
    information:
        - flow_run_id: the UUID of the flow run
        - flow_name: the name of the flow
        - task_run_id: the UUID of the task run (if available)

    If Prefect context information is not available, the original logger is returned.

    Returns:
        structlog.BoundLogger: The enriched logger
    """
    try:
        ctx = get_run_context()
        return logger.bind(
            flow_run_id=str(ctx.flow_run.id),
            flow_name=ctx.flow_run.name,
            task_run_id=getattr(ctx.task_run, 'id', None)
        )
    except Exception:
        # If Prefect context information is not available, return the original logger
        return logger

@task(retries=3, retry_delay_seconds=10)
def process_endpoint(endpoint: str, table_name: str, pk_col: str):
    """
    Process an endpoint from the SpaceX API and load the data into a PostgreSQL database.

    The task will retry up to 3 times if an exception occurs, with a delay of 10 seconds between retries.

    Args:
        endpoint (str): The endpoint to process (e.g. "rockets", "launchpads", etc.)
        table_name (str): The name of the table in the PostgreSQL database to load the data into.
        pk_col (str): The name of the primary key column in the table.

    Raises:
        Exception: If an exception occurs during the processing of the endpoint, it will be raised.
    """
    prefect_logger = get_run_logger()
    
    log = get_enriched_logger().bind(endpoint=endpoint, table=table_name)
    
    log.info("Iniciando processamento de domínio")
    prefect_logger.info(f"Task iniciada para: {endpoint}")

    try:
        # Fetch data from the SpaceX API
        raw_data = SpaceXExtractor.fetch_data(endpoint)
        
        # Transform the data using the appropriate method
        transform_method = getattr(SpaceXTransformer, f"transform_{endpoint}")
        df_clean = transform_method(raw_data)
        
        # Load the transformed data into the PostgreSQL database
        PostgresLoader.upsert_dataframe(df_clean, table_name, pk_col)
        
        log.info("Domínio processado com sucesso")
    
    except Exception as e:
        # Log an error message if an exception occurs
        log.error("Falha no processamento do domínio", error=str(e))
        # Also log the error with the Prefect logger
        prefect_logger.error(f"Erro crítico em {endpoint}: {e}")
        # Raise the exception so that it can be handled by the task runner
        raise

@flow(name="SpaceX ETL Production Pipeline")
def spacex_etl_flow():
    """
    The main flow that processes data from the SpaceX API and loads it into a PostgreSQL database.

    The flow processes the endpoints in the following order:

    1. "rockets"
    2. "launchpads"
    3. "payloads" (waits for "rockets" and "launchpads" to finish)
    4. "launches" (waits for "rockets" and "launchpads" to finish)

    Args:
        None

    Returns:
        None
    """
    log = get_enriched_logger()
    log.info("Iniciando Flow de Produção SpaceX")

    # Process the "rockets" endpoint
    rockets_future = process_endpoint.submit("rockets", "rockets", "rocket_id")
    log.info("Processamento do endpoint 'rockets' iniciado")

    # Process the "launchpads" endpoint
    launchpads_future = process_endpoint.submit("launchpads", "launchpads", "launchpad_id")
    log.info("Processamento do endpoint 'launchpads' iniciado")

    # Process the "payloads" endpoint and wait for "rockets" and "launchpads" to finish
    payloads_future = process_endpoint.submit(
        "payloads", "payloads", "payload_id", 
        wait_for=[rockets_future, launchpads_future]
    )
    log.info("Processamento do endpoint 'payloads' iniciado (aguardando para os endpoints 'rockets' e 'launchpads' terminarem)")

    # Process the "launches" endpoint and wait for "rockets" and "launchpads" to finish
    launches_future = process_endpoint.submit(
        "launches", "launches", "launch_id", 
        wait_for=[rockets_future, launchpads_future]
    )
    log.info("Processamento do endpoint 'launches' iniciado (aguardando para os endpoints 'rockets' e 'launchpads' terminarem)")

if __name__ == "__main__":
    spacex_etl_flow()