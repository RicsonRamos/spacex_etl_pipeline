from prefect import flow, task
from src.extract.spacex_api import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.loader import PostgresLoader
import structlog
from typing import List, Dict, Any
import polars as pl

logger = structlog.get_logger()

@task(name="Extraction", retries=2)
def extract_task(endpoint: str) -> List[Dict[str, Any]]:
    """
    Task responsible for extracting data from the SpaceX API.

    Args:
        endpoint (str): The endpoint to extract data from (e.g. "rockets", "launchpads", etc.)

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing the extracted data.

    Raises:
        requests.exceptions.HTTPError: If the API returns a status code other than 200.
    """
    return SpaceXExtractor().fetch_data(endpoint)

@task(name="Transformation")
def transform_task(data: List[Dict[str, Any]], endpoint: str) -> pl.DataFrame:
    """
    Task responsible for transforming the extracted data into a pandas DataFrame.

    Args:
        data (List[Dict[str, Any]]): The extracted data in the form of a list of dictionaries.
        endpoint (str): The endpoint from which the data was extracted (e.g. "rockets", "launchpads", etc.).

    Returns:
        pl.DataFrame: A pandas DataFrame containing the transformed data.

    Raises:
        AttributeError: If the Transformer does not have a method with the name transform_<endpoint>.
    """
    transformer = SpaceXTransformer()
    
    # Dynamically call transform_rockets, transform_launches, etc.
    method_name = f"transform_{endpoint}"
    if hasattr(transformer, method_name):
        return getattr(transformer, method_name)(data)
    raise AttributeError(f"Method {method_name} not found in Transformer.")

@task(name="Loading")
def load_task(df: pl.DataFrame, table_name: str, pk: str):
    """
    Task responsible for loading the transformed data into a PostgreSQL database.

    Args:
        df (pl.DataFrame): The transformed data in the form of a pandas DataFrame.
        table_name (str): The name of the table in the PostgreSQL database to load the data into.
        pk (str): The name of the primary key column in the table.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: If an error occurs during the loading process.
    """
    loader = PostgresLoader()
    loader.upsert_dataframe(df, table_name, pk)

@flow(name="SpaceX_ETL_Full")
def spacex_etl_flow():
    """
    Main flow: Ensures that the dim tables (rockets and launchpads) are loaded 
    before the fact table (launches).
    """
    loader = PostgresLoader()
    loader.ensure_tables()

    # 1. Load Rockets (Dim table)
    raw_rockets = extract_task("rockets")
    df_rockets = transform_task(raw_rockets, "rockets")
    load_task(df_rockets, "rockets", "rocket_id")
    logger.info("Loaded rockets dim table.")

    # 2. Load Launchpads (Dim table)
    raw_pads = extract_task("launchpads")
    df_pads = transform_task(raw_pads, "launchpads")
    load_task(df_pads, "launchpads", "launchpad_id")
    logger.info("Loaded launchpads dim table.")

    # 3. Load Launches (Fact table - depends on rockets and launchpads)
    raw_launches = extract_task("launches")
    df_launches = transform_task(raw_launches, "launches")
    load_task(df_launches, "launches", "launch_id")
    logger.info("Loaded launches fact table.")
    logger.info("ETL finished successfully and referential integrity maintained.")

if __name__ == "__main__":
    spacex_etl_flow.serve(
        name="spacex-etl-prod",
        tags=["production"],
        cron="0 3 * * *"
    )