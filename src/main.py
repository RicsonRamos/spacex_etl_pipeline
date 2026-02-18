from prefect import flow, task
from src.extract.spacex_api import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.loader import PostgresLoader, AlertHandler
from src.config.settings import settings
from src.database.models import Base
import structlog
import polars as pl
from typing import List, Optional

logger = structlog.get_logger()

def get_loader():
    """Instancia o loader com tratamento de alertas."""
    alerts = AlertHandler(slack_webhook_url=settings.SLACK_WEBHOOK_URL)
    return PostgresLoader(alert_handler=alerts)

@task(name="Extraction", retries=2, retry_delay_seconds=10)
def extract_task(endpoint: str):
    logger.info(f"Iniciando extração do endpoint: {endpoint}")
    return SpaceXExtractor().fetch_data(endpoint)

@task(name="Transformation")
def transform_task(data: list, endpoint: str, valid_ids: Optional[List[str]] = None):
    """
    RIGOR: Agora aceita valid_ids opcionalmente para integridade referencial.
    """
    logger.info(f"Transformando dados de: {endpoint}")
    transformer = SpaceXTransformer()
    
    # O getattr busca o método dentro da instância da classe corrigida
    method_name = f"transform_{endpoint}"
    method = getattr(transformer, method_name, None)
    
    if not method:
        raise AttributeError(f"Método {method_name} não encontrado na classe SpaceXTransformer. Verifique a indentação no arquivo transformer.py.")

    # Se for payloads, passamos a lista de IDs para o filtro de órfãos
    if endpoint == "payloads" and valid_ids is not None:
        return method(data, valid_launch_ids=valid_ids)
    
    return method(data)

@task(name="Load_to_Bronze")
def load_bronze_task(data: list, endpoint: str):
    loader = get_loader()
    loader.load_to_bronze(data, f"bronze_{endpoint}")

@task(name="Load_to_Silver")
def load_silver_task(df: pl.DataFrame, table_name: str, pk: str):
    if df is None or df.is_empty():
        logger.warning(f"DataFrame para {table_name} está vazio. Abortando carga.")
        return
    
    loader = get_loader()
    logger.info(f"Carregando {len(df)} registros em {table_name}")
    loader.load_to_silver(df, table_name, pk)

@task(name="Refresh_Gold")
def refresh_gold_task():
    logger.info("Atualizando camada Gold (Views/Materialized Views)")
    loader = get_loader()
    loader.refresh_gold_layer()

@flow(name="SpaceX_Medallion_Pipeline")
def spacex_etl_flow():
    loader = get_loader()
    
    logger.info("Validando schema do banco de dados...")
    Base.metadata.create_all(loader.engine)

    # Configuração de endpoints e suas PKs
    endpoints_config = [
        {"ep": "rockets", "pk": "rocket_id"},
        {"ep": "launchpads", "pk": "launchpad_id"},
        {"ep": "launches", "pk": "launch_id"},
        {"ep": "payloads", "pk": "payload_id"}
    ]

    valid_launch_ids = None

    for config in endpoints_config:
        ep = config["ep"]
        pk = config["pk"]
        
        # 1. Extração
        raw_data = extract_task(ep)
        
        # 2. Bronze
        load_bronze_task(raw_data, ep)
        
        # 3. Transformação (com lógica de integridade para payloads)
        df_transformed = transform_task(raw_data, ep, valid_ids=valid_launch_ids)
        
        # 4. Silver
        load_silver_task(df_transformed, ep, pk)

        # Se acabamos de processar lançamentos, guardamos os IDs para os payloads
        if ep == "launches" and not df_transformed.is_empty():
            valid_launch_ids = df_transformed["launch_id"].to_list()

    # 5. Gold
    refresh_gold_task()
    logger.info("Pipeline executado com sucesso!")

if __name__ == "__main__":
    spacex_etl_flow()