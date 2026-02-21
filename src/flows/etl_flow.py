import structlog
import requests
import subprocess
import psycopg2
import polars as pl
import json

from datetime import datetime, timezone
from typing import Dict, List

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from psycopg2.extras import execute_batch
from prometheus_client import Counter, Histogram, start_http_server
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.extract.schemas import ENDPOINT_SCHEMAS
from src.transform.transformer import SpaceXTransformer
from src.config.settings import settings


# =====================================================
# CONSTANTS
# =====================================================

DEFAULT_START = datetime(2000, 1, 1, tzinfo=timezone.utc)


# =====================================================
# LOGGING
# =====================================================

logger = structlog.get_logger()


# =====================================================
# METRICS
# =====================================================

EXTRACT_COUNT = Counter(
    "extract_count",
    "Registros extraídos",
    ["endpoint"]
)

BRONZE_COUNT = Counter(
    "bronze_count",
    "Registros no Bronze",
    ["endpoint"]
)

SILVER_COUNT = Counter(
    "silver_count",
    "Registros na Silver",
    ["endpoint"]
)

EXECUTION_TIME = Histogram(
    "flow_execution_seconds",
    "Tempo de execução",
    ["flow"]
)

start_http_server(8000)


# =====================================================
# SLACK
# =====================================================

def slack_notify(msg: str):

    if not settings.SLACK_WEBHOOK_URL:
        return

    try:
        requests.post(
            settings.SLACK_WEBHOOK_URL,
            json={"text": msg},
            timeout=10
        )

    except Exception:
        logger.warning("Falha ao enviar Slack")


# =====================================================
# POSTGRES CLIENT
# =====================================================

class PostgresClient:

    def __init__(self):

        self.conn = psycopg2.connect(
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT,
            dbname=settings.POSTGRES_DB,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD
        )

        self.conn.autocommit = False


    def close(self):
        self.conn.close()


    # -----------------------------

    def get_last_ingested(
        self,
        table: str,
        col: str = "ingested_at"
    ) -> datetime | None:

        q = f"SELECT MAX({col}) FROM {table}"

        with self.conn.cursor() as cur:
            cur.execute(q)
            return cur.fetchone()[0]


    # -----------------------------
    # BRONZE → JSONB
    # -----------------------------

    def insert_bronze(
        self,
        table: str,
        data: List[Dict]
    ):

        if not data:
            return

        records = [
            {"raw_data": json.dumps(row, default=str)}
            for row in data
        ]

        query = f"""
            INSERT INTO {table} (raw_data)
            VALUES (%(raw_data)s)
            ON CONFLICT DO NOTHING
        """

        with self.conn.cursor() as cur:
            execute_batch(
                cur,
                query,
                records,
                page_size=1000
            )


    # -----------------------------
    # SILVER UPSERT
    # -----------------------------

    def upsert_silver(
        self,
        table: str,
        df: pl.DataFrame,
        pk: str
    ):

        if df.is_empty():
            return

        df = df.with_columns(
            pl.lit(
                datetime.now(timezone.utc)
            ).alias("ingested_at")
        )

        rows = df.to_dicts()
        cols = list(rows[0].keys())

        update = ",".join([
            f"{c}=EXCLUDED.{c}"
            for c in cols
            if c != pk
        ])

        query = f"""
            INSERT INTO {table}
            ({','.join(cols)})
            VALUES ({','.join(['%s']*len(cols))})
            ON CONFLICT ({pk})
            DO UPDATE SET {update}
        """

        values = [
            tuple(r[c] for c in cols)
            for r in rows
        ]

        with self.conn.cursor() as cur:
            execute_batch(
                cur,
                query,
                values,
                page_size=500
            )


    # -----------------------------

    def commit(self):
        self.conn.commit()


    def rollback(self):
        self.conn.rollback()


# =====================================================
# HTTP CLIENT
# =====================================================

class HTTPClient:

    def __init__(self):

        session = requests.Session()

        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503],
            allowed_methods=["GET"]
        )

        adapter = HTTPAdapter(max_retries=retry)

        session.mount("https://", adapter)

        self.session = session


    def get(self, url: str):

        r = self.session.get(
            url,
            timeout=settings.API_TIMEOUT
        )

        r.raise_for_status()

        return r.json()


# =====================================================
# TASKS
# =====================================================

@task(retries=5, retry_delay_seconds=15)
def extract_task(
    endpoint: str,
    last: datetime
):

    from src.extract.spacex_api import SpaceXExtractor

    extractor = SpaceXExtractor()

    data = extractor.fetch(
        endpoint,
        incremental=True,
        last_ingested=last
    )

    EXTRACT_COUNT.labels(endpoint).inc(len(data))

    return data


# -----------------------------

@task(retries=3)
def bronze_task(
    endpoint: str,
    data: List[Dict]
):

    db = PostgresClient()

    try:

        db.insert_bronze(
            f"bronze_{endpoint}",
            data
        )

        db.commit()

        BRONZE_COUNT.labels(endpoint).inc(len(data))

    except Exception:

        db.rollback()
        raise

    finally:
        db.close()


# -----------------------------

@task(retries=3)
def transform_task(
    endpoint: str,
    data: List[Dict]
) -> pl.DataFrame:

    transformer = SpaceXTransformer()

    return transformer.transform(endpoint, data)


# -----------------------------

@task(retries=3)
def silver_task(
    endpoint: str,
    df: pl.DataFrame,
    pk: str
):

    if pk not in df.columns:
        raise ValueError(f"PK ausente: {pk}")

    db = PostgresClient()

    try:

        db.upsert_silver(
            f"silver_{endpoint}",
            df,
            pk
        )

        db.commit()

        SILVER_COUNT.labels(endpoint).inc(len(df))

    except Exception:

        db.rollback()
        raise

    finally:
        db.close()


# =====================================================
# FLOWS
# =====================================================

@flow(
    name="Bronze Layer",
    task_runner=ConcurrentTaskRunner()
)
def bronze_flow(
    endpoints: List[str],
    last_map: Dict[str, datetime]
):

    futures = {}

    for ep in endpoints:

        f = extract_task.submit(
            ep,
            last_map[ep]
        )

        bronze_task.submit(ep, f)

        futures[ep] = f

    return {
        k: v.result()
        for k, v in futures.items()
    }


# -----------------------------

@flow(
    name="Silver Layer",
    task_runner=ConcurrentTaskRunner()
)
def silver_flow(
    bronze: Dict[str, list],
    pk_map: Dict[str, str]
):

    for ep, data in bronze.items():

        df = transform_task.submit(ep, data)

        silver_task.submit(
            ep,
            df,
            pk_map[ep]
        )


# -----------------------------

@task(retries=2, retry_delay_seconds=30)
def run_dbt():

    cmd = [
        "dbt",
        "build",
        "--project-dir", "/app/dbt",
        "--profiles-dir", "/app/dbt"
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=1800
    )

    if result.returncode != 0:

        logger.error("DBT erro", stderr=result.stderr)

        raise RuntimeError("DBT failed")


# -----------------------------

@flow(name="Gold Layer")
def gold_flow():

    run_dbt()


# =====================================================
# MAIN PIPELINE
# =====================================================

@flow(name="SpaceX Enterprise ETL")
def spacex_pipeline():

    endpoints = [
        "rockets",
        "launchpads",
        "launches"
    ]

    pk_map = {
        "rockets": "rocket_id",
        "launchpads": "launchpad_id",
        "launches": "launch_id"
    }

    db = PostgresClient()

    try:

        last_map = {
            ep: db.get_last_ingested(
                f"silver_{ep}"
            ) or DEFAULT_START
            for ep in endpoints
        }

    finally:
        db.close()

    bronze = bronze_flow(
        endpoints,
        last_map
    )

    silver_flow(
        bronze,
        pk_map
    )

    gold_flow()