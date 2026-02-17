from prefect import flow, task
from src.extract.spacex_api import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.loader import PostgresLoader, AlertHandler
from src.config.settings import settings
from src.database.models import Base
import structlog
import polars as pl

logger = structlog.get_logger()

# RIGOR: Não instancie o loader globalmente para evitar erros de 'pickle'
def get_loader():
    alerts = AlertHandler(slack_webhook_url=settings.SLACK_WEBHOOK_URL)
    return PostgresLoader(alert_handler=alerts)

@task(name="Extraction", retries=2)
def extract_task(endpoint: str):
    return SpaceXExtractor().fetch_data(endpoint)

@task(name="Transformation")
def transform_task(data: list, endpoint: str):
    transformer = SpaceXTransformer()
    method_name = f"transform_{endpoint}"
    if hasattr(transformer, method_name):
        return getattr(transformer, method_name)(data)
    raise AttributeError(f"Method {method_name} not found.")

@task(name="Load_to_Bronze")
def load_bronze_task(data: list, endpoint: str):
    loader = get_loader()
    loader.load_to_bronze(data, endpoint)

@task(name="Load_to_Silver")
def load_silver_task(df: pl.DataFrame, table_name: str, pk: str):
    loader = get_loader()
    loader.load_to_silver(df, table_name, pk)

@task(name="Refresh_Gold")
def refresh_gold_task():
    loader = get_loader()
    loader.refresh_gold_layer()

@flow(name="SpaceX_Medallion_Pipeline")
def spacex_etl_flow():
    loader = get_loader()
    Base.metadata.create_all(loader.engine)

    endpoints = {
        "rockets": "rocket_id",
        "launchpads": "launchpad_id",
        "payloads": "payload_id",
        "launches": "launch_id"
    }

    for ep, pk in endpoints.items():
        # 1. BRONZE
        raw_data = extract_task(ep)
        load_bronze_task(raw_data, ep)
        
        # 2. SILVER
        df_transformed = transform_task(raw_data, ep)
        load_silver_task(df_transformed, ep, pk)

    # 3. GOLD
    refresh_gold_task()

if __name__ == "__main__":
    # COMENTE O SERVE PARA TESTAR:
    spacex_etl_flow.serve(name="spacex-etl-prod")
    
    # RODE DIRETAMENTE:
    #spacex_etl_flow()