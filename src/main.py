from prefect import flow, task
from src.extract.spacex_api import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.loader import PostgresLoader
import structlog

logger = structlog.get_logger()

@task(name="Extraction", retries=2)
def extract_task(endpoint: str):
    return SpaceXExtractor().fetch_data(endpoint)

@task(name="Transformation")
def transform_task(data: list, endpoint: str):
    transformer = SpaceXTransformer()
    # Chama dinamicamente transform_rockets, transform_launches, etc.
    method_name = f"transform_{endpoint}"
    if hasattr(transformer, method_name):
        return getattr(transformer, method_name)(data)
    raise AttributeError(f"Método {method_name} não encontrado no Transformer.")

@task(name="Loading")
def load_task(df, table_name: str, pk: str):
    loader = PostgresLoader()
    loader.upsert_dataframe(df, table_name, pk)

@flow(name="SpaceX_ETL_Full")
def spacex_etl_flow():
    """
    Fluxo principal: Garante que os pais (Dimensões) sejam carregados 
    antes dos filhos (Fatos/Launches).
    """
    loader = PostgresLoader()
    loader.ensure_tables()

    # 1. Carregar Foguetes (Dimensão)
    raw_rockets = extract_task("rockets")
    df_rockets = transform_task(raw_rockets, "rockets")
    load_task(df_rockets, "rockets", "rocket_id")

    # 2. Carregar Plataformas (Dimensão)
    raw_pads = extract_task("launchpads")
    df_pads = transform_task(raw_pads, "launchpads")
    load_task(df_pads, "launchpads", "launchpad_id")

    # 3. Carregar Lançamentos (Fato - depende de rockets e launchpads)
    raw_launches = extract_task("launches")
    df_launches = transform_task(raw_launches, "launches")
    load_task(df_launches, "launches", "launch_id")

    logger.info("ETL concluído com sucesso e integridade referencial mantida!")

if __name__ == "__main__":
    spacex_etl_flow.serve(
        name="spacex-etl-prod",
        tags=["production"],
        cron="0 3 * * *"
    )