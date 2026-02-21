import structlog
from prefect import flow, task
from prefect.futures import wait
from prefect.task_runners import ConcurrentTaskRunner

from src.extract.spacex_api import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.loader import PostgresLoader

logger = structlog.get_logger()



# TASKS


@task(retries=3, retry_delay_seconds=10)
def extract_task(endpoint: str) -> list:
    extractor = SpaceXExtractor()
    logger.info("Extracting", endpoint=endpoint)
    return extractor.fetch(endpoint)


@task(retries=3, retry_delay_seconds=10)
def load_bronze_task(endpoint: str, data: list) -> None:
    loader = PostgresLoader()
    loader.load_bronze(data, f"bronze_{endpoint}")
    logger.info("Bronze loaded", endpoint=endpoint, records=len(data))


@task(retries=2, retry_delay_seconds=5)
def transform_task(endpoint: str, data: list):
    transformer = SpaceXTransformer()
    logger.info("Transforming", endpoint=endpoint)
    return transformer.transform(endpoint, data)


@task(retries=2, retry_delay_seconds=5)
def load_silver_task(endpoint: str, df, pk: str):
    loader = PostgresLoader()
    loader.upsert_silver(df, f"silver_{endpoint}", pk)
    logger.info("Silver updated", endpoint=endpoint)


@task
def refresh_gold_task():
    loader = PostgresLoader()

    gold_definition = """
        SELECT 
            r.name AS rocket_name,
            COUNT(l.launch_id) AS total_launches,
            AVG(CAST(l.success AS INT)) * 100 AS success_rate
        FROM silver_rockets r
        LEFT JOIN silver_launches l ON r.rocket_id = l.rocket_id
        GROUP BY r.name
    """

    loader.refresh_gold_view("gold_rocket_performance", gold_definition)
    logger.info("Gold view refreshed")



# SUBFLOW - BRONZE


@flow(name="Bronze Layer", task_runner=ConcurrentTaskRunner())
def bronze_flow(endpoints: list[str]) -> dict[str, list]:

    futures = {}
    load_futures = []

    for endpoint in endpoints:
        data_future = extract_task.submit(endpoint)
        load_future = load_bronze_task.submit(endpoint, data_future)

        futures[endpoint] = data_future
        load_futures.append(load_future)

    wait(list(futures.values()) + load_futures)


    for f in list(futures.values()) + load_futures:
        f.result()

    logger.info("Bronze layer completed")
    return {k: v.result() for k, v in futures.items()}



# SUBFLOW - SILVER


@flow(name="Silver Layer", task_runner=ConcurrentTaskRunner())
def silver_flow(bronze_data: dict[str, list], pk_map: dict[str, str]):

    futures = []

    for endpoint, data in bronze_data.items():
        transform_future = transform_task.submit(endpoint, data)

        silver_future = load_silver_task.submit(
            endpoint,
            transform_future,
            pk_map[endpoint],
        )

        futures.append(silver_future)

    # Espera terminar
    wait(futures)

    
    for future in futures:
        future.result()

    logger.info("Silver layer completed")


# SUBFLOW - GOLD


@flow(name="Gold Layer")
def gold_flow():
    future = refresh_gold_task.submit()
    future.result()  # força erro subir
    logger.info("Gold layer completed")



# MASTER FLOW (ORCHESTRATOR)


@flow(name="SpaceX Medallion Pipeline - Production")
def spacex_etl_pipeline():

    logger.info("Starting SpaceX Medallion Pipeline")

    endpoints = ["rockets", "launchpads", "launches"]

    pk_map = {
        "rockets": "rocket_id",
        "launchpads": "launchpad_id",
        "launches": "launch_id",
    }

    try:
        bronze_data = bronze_flow(endpoints)
        silver_flow(bronze_data, pk_map)
        gold_flow()

        logger.info("Pipeline finished successfully 🚀")

    except Exception as e:
        logger.error("Pipeline failed ", error=str(e))
        raise  


if __name__ == "__main__":
    spacex_etl_pipeline()