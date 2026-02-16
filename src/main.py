from prefect import flow, task
from src.extract.spacex_api import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.loader import PostgresLoader
import polars as pl

@task(retries=2, name="Extraction")
def extract():
    """
    Extract data from the SpaceX API.

    This task fetches data from the SpaceX API and returns it as a list of dictionaries.
    The data is retrieved from the "launches" endpoint.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing data fetched from the SpaceX API.
    """
    return SpaceXExtractor().fetch_data("launches")

@task(name="Transformation")
def transform(data):
    """
    Transform the extracted data into a structure suitable for database loading.

    This task converts the raw extraction into a structured format for database insertion.
    The extraction is transformed into a Polars DataFrame, and it verifies that the "launch_id" column exists.
    If the "launch_id" column is missing, a KeyError is raised.

    Args:
        data (List[Dict[str, Any]]): A list of dictionaries containing the extracted data.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the transformed data.

    Raises:
        KeyError: If the "launch_id" column is not found in the DataFrame.
    """
    df = SpaceXTransformer().transform_launches(data)
    if "launch_id" not in df.columns:
        raise KeyError("Schema error: 'launch_id' is missing.")
    return df

@task(name="Load")
def load(df: pl.DataFrame):
    """
    Load the DataFrame into the database.

    This task loads the transformed DataFrame into the database.
    It ensures the target table exists before inserting data.
    If the table does not exist, it is created with a structure suitable for the DataFrame.

    Args:
        df (pl.DataFrame): The Polars DataFrame containing the transformed data.

    Returns:
        None
    """
    loader = PostgresLoader()
    # Ensure the table exists before upserting
    loader.ensure_table()
    # Load the DataFrame into the database
    loader.upsert_dataframe(df, "launches", "launch_id")

@flow(name="SpaceX_ETL")
def main_flow():
    """
    Main ETL flow for SpaceX data.

    This flow extracts data from the SpaceX API, transforms it into a format
    suitable for loading into a PostgreSQL database, and then loads it into the database.

    The flow consists of three tasks: extract, transform, and load.
    """
    raw_data = extract()
    """
    Raw data extracted from the SpaceX API.

    This data is a list of dictionaries, where each dictionary contains information about a launch.
    """
    transformed_df = transform(raw_data)
    """
    Transformed data ready for loading into a PostgreSQL database.

    This is a Polars DataFrame with columns representing different launch details.
    """
    load(transformed_df)
    """
    Loads the transformed data into a PostgreSQL database.

    Ensures the table exists before loading. If it doesn't exist, it is created.
    """

@flow(name="SpaceX_ETL")
def spacex_etl_flow():
    """
    Main ETL flow for SpaceX data (alternative entry point).

    This flow extracts, transforms, and loads SpaceX launch data into a PostgreSQL database.
    """
    # Extract data
    raw_data = extract()
    # Transform data
    transformed_df = transform(raw_data)
    # Load data
    load(transformed_df)

if __name__ == "__main__":
    spacex_etl_flow.serve(
        name="spacex-etl-prod",
        tags=["local-machine"],
        parameters={},
        cron="0 3 * * *"  # Runs every day at 3 AM
    )
