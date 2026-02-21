import structlog
from datetime import datetime
from prefect import flow, task
from prefect.futures import wait
from prefect.task_runners import ConcurrentTaskRunner  # Apenas o que existe
# from prefect.notifications import SlackNotification  # Prefect 2.x não tem mais SlackNotification nativo
import polars as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from prometheus_client import Counter, Histogram, start_http_server
# from great_expectations.dataset import PandasDataset  # Deprecado; use great_expectations.dataset.new_api se quiser GE moderno
import subprocess
import time

from src.extract.schemas import ENDPOINT_SCHEMAS
from src.transform.transformer import SpaceXTransformer
from src.config.settings import settings

logger = structlog.get_logger()

# ----------------------
# PROMETHEUS METRICS
# ----------------------
EXTRACT_COUNT = Counter("extract_count", "Registros extraídos", ["endpoint"])
BRONZE_COUNT = Counter("bronze_count", "Registros carregados no Bronze", ["endpoint"])
SILVER_COUNT = Counter("silver_count", "Registros carregados na Silver", ["endpoint"])
EXECUTION_TIME = Histogram("flow_execution_seconds", "Tempo de execução dos fluxos", ["flow_name"])
start_http_server(8000)

# ----------------------
# SLACK NOTIFIER (prefect 2.x não tem SlackNotification)
# Use webhook direto
def slack_notify(message: str):
    import requests
    if settings.SLACK_WEBHOOK_URL:
        requests.post(settings.SLACK_WEBHOOK_URL, json={"text": message})

# ----------------------
# EXTRACTOR COM RETRY INTELIGENTE
# ----------------------
class SpaceXExtractor:
    def __init__(self):
        self.session = self._setup_session()

    def _setup_session(self) -> requests.Session:
        session = requests.Session()
        retries = Retry(
            total=settings.API_RETRIES,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session

    def fetch(self, endpoint: str, incremental: bool = False, last_ingested: datetime | None = None) -> list:
        url = f"{settings.SPACEX_API_URL}/{endpoint}"
        try:
            logger.info("Iniciando extração", endpoint=endpoint)
            response = self.session.get(url, timeout=settings.API_TIMEOUT)
            response.raise_for_status()
            data = response.json()

            if incremental and last_ingested:
                data = [
                    item for item in data
                    if item.get("date_utc") and datetime.fromisoformat(item["date_utc"].replace("Z", "+00:00")) > last_ingested
                ]

            schema = ENDPOINT_SCHEMAS.get(endpoint)
            if schema:
                data = [schema(**item).model_dump() for item in data]
            else:
                logger.warning("Schema não encontrado, retornando dados brutos", endpoint=endpoint)

            logger.info("Extração concluída", endpoint=endpoint, count=len(data))
            return data

        except requests.exceptions.Timeout as e:
            logger.warning("Timeout na extração", endpoint=endpoint)
            raise
        except requests.exceptions.HTTPError as e:
            logger.warning("Erro HTTP na extração", endpoint=endpoint, status=e.response.status_code)
            raise
        except Exception as e:
            logger.error("Falha inesperada na extração", endpoint=endpoint, error=str(e))
            raise

# ----------------------
# LOADER COM VERSIONAMENTO/AUDITORIA
# ----------------------
class PostgresLoader:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT,
            dbname=settings.POSTGRES_DB,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD
        )
        self.conn.autocommit = True

    def get_last_ingested(self, table_name: str, timestamp_column: str = "ingested_at"):
        query = f"SELECT MAX({timestamp_column}) AS last_ingested FROM {table_name}"
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            result = cur.fetchone()
            return result["last_ingested"] if result["last_ingested"] else None

    def load_bronze(self, data: list, table_name: str):
        if not data:
            logger.info("Nenhum dado novo para Bronze", table=table_name)
            return
        columns = data[0].keys()
        placeholders = ", ".join([f"%({c})s" for c in columns])
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        with self.conn.cursor() as cur:
            for row in data:
                cur.execute(query, row)

    def upsert_silver_with_version(self, df: pd.DataFrame, table_name: str, pk: str):
        if df.empty:
            logger.info("Nenhum dado para Silver", table=table_name)
            return
        df["ingested_at"] = pd.Timestamp.now()
        with self.conn.cursor() as cur:
            for _, row in df.iterrows():
                columns = df.columns.tolist()
                placeholders = ", ".join([f"%({col})s" for col in columns])
                updates = ", ".join([f"{col}=EXCLUDED.{col}" for col in columns if col != pk])
                query = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES ({placeholders})
                    ON CONFLICT ({pk}) DO UPDATE SET {updates}
                """
                cur.execute(query, row.to_dict())

# ----------------------
# TASKS PREFECT
# ----------------------
@task
def validate_silver(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    # Great Expectations removed for simplificação
    return df

def exponential_backoff(attempt: int):
    return min(60, 2 ** attempt)

@task(retries=5, retry_delay_seconds=10)
def extract_task(endpoint: str, last_ingested: datetime | None = None) -> list:
    attempt = 0
    while attempt < 5:
        try:
            extractor = SpaceXExtractor()
            data = extractor.fetch(endpoint, incremental=bool(last_ingested), last_ingested=last_ingested)
            EXTRACT_COUNT.labels(endpoint=endpoint).inc(len(data))
            return data
        except (requests.exceptions.Timeout, requests.exceptions.HTTPError) as e:
            delay = exponential_backoff(attempt)
            logger.warning(f"Retry {attempt+1} em {delay}s devido a {e}", endpoint=endpoint)
            time.sleep(delay)
            attempt += 1
    raise RuntimeError(f"Falha na extração após retries: {endpoint}")

@task(retries=3, retry_delay_seconds=10)
def load_bronze_task(endpoint: str, data: list):
    loader = PostgresLoader()
    loader.load_bronze(data, f"bronze_{endpoint}")
    BRONZE_COUNT.labels(endpoint=endpoint).inc(len(data))

@task(retries=2, retry_delay_seconds=5)
def transform_task(endpoint: str, data: list):
    transformer = SpaceXTransformer()
    return transformer.transform(endpoint, data)

@task(retries=2, retry_delay_seconds=5)
def load_silver_task(endpoint: str, df: pd.DataFrame, pk: str):
    loader = PostgresLoader()
    loader.upsert_silver_with_version(df, f"silver_{endpoint}", pk)
    SILVER_COUNT.labels(endpoint=endpoint).inc(len(df))

# ----------------------
# FLOWS
# ----------------------
@flow(name="Bronze Layer", task_runner=ConcurrentTaskRunner())
def bronze_flow(endpoints: list[str], last_ingested_map: dict[str, datetime | None]) -> dict[str, list]:
    futures, load_futures = {}, []
    for ep in endpoints:
        f_data = extract_task.submit(ep, last_ingested_map.get(ep))
        f_load = load_bronze_task.submit(ep, f_data)
        futures[ep] = f_data
        load_futures.append(f_load)
    wait(list(futures.values()) + load_futures)
    return {k: v.result() for k, v in futures.items()}

@flow(name="Silver Layer", task_runner=ConcurrentTaskRunner())
def silver_flow(bronze_data: dict[str, list], pk_map: dict[str, str]):
    futures = []
    for ep, data in bronze_data.items():
        df_transformed = transform_task.submit(ep, data)
        df_validated = validate_silver.submit(df_transformed, ep)
        f_silver = load_silver_task.submit(ep, df_validated, pk_map[ep])
        futures.append(f_silver)
    wait(futures)

@flow(name="Gold Layer")
def gold_flow():
    subprocess.run(["dbt", "run", "--models", "gold_rocket_performance"], check=True)

@flow(name="SpaceX ETL Pipeline - Enterprise")
def spacex_etl_pipeline():
    endpoints = ["rockets", "launchpads", "launches"]
    pk_map = {"rockets": "rocket_id", "launchpads": "launchpad_id", "launches": "launch_id"}
    loader = PostgresLoader()
    last_ingested_map = {ep: loader.get_last_ingested(f"silver_{ep}", "ingested_at") for ep in endpoints}

    bronze_data = bronze_flow(endpoints, last_ingested_map)
    silver_flow(bronze_data, pk_map)
    gold_flow()