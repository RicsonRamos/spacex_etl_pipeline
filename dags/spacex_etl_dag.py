from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import os
import sys
import logging
import json

# ----------------------------
# LOGGER E FORMATAÇÃO JSON
# ----------------------------
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "time": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "module": record.module,
            "message": record.getMessage(),
            "name": record.name,
        }
        return json.dumps(log_record)

def get_logger(name: str, json_logs: bool = True):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = JsonFormatter() if json_logs else logging.Formatter(
            '%(asctime)s - %(levelname)s - %(module)s - %(message)s'
        )
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger

logger = get_logger("airflow.pipeline")

# ----------------------------
# CALLBACK DE FALHA
# ----------------------------
def on_failure_callback(context):
    ti = context.get('task_instance')
    alert_email = os.getenv('ALERT_EMAIL')
    
    log_msg = {
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": ti.run_id,
        "status": "FAILED",
        "log_url": ti.log_url
    }
    logger.error(json.dumps(log_msg))
    
    if alert_email:
        subject = f"DAG {ti.dag_id} | TASK {ti.task_id} FAILED"
        body = f"""
        DAG: {ti.dag_id}<br>
        Task: {ti.task_id}<br>
        Run ID: {ti.run_id}<br>
        Log URL: <a href='{ti.log_url}'>Clique aqui para logs</a>
        """
        send_email(to=alert_email, subject=subject, html_content=body)

# ----------------------------
# CONFIGURAÇÃO GERAL
# ----------------------------
DBT_PROJECT_PATH = os.getenv('DBT_PROJECT_PATH_ON_HOST', '/usr/app')
DOCKER_API_VERSION = '1.44'
NETWORK_NAME = 'spacex_etl_pipeline_default'

default_args = {
    'owner': 'airflow',
    'on_failure_callback': on_failure_callback,
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 3,
    'retry_delay': timedelta(seconds=60),
    'retry_exponential_backoff': True,
}

# ----------------------------
# DAG PRINCIPAL
# ----------------------------
with DAG(
    'spacex_full_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['spacex', 'dbt', 'infrastructure']
) as dag:

    # ----------------------------
    # TASK 1: Ingestão de Dados
    # ----------------------------
    ingest_data = DockerOperator(
        task_id='ingest_data',
        image='spacex_etl_pipeline-ingestion_engine:latest',
        api_version=DOCKER_API_VERSION,
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode=NETWORK_NAME,
        mount_tmp_dir=False,
        sla=timedelta(minutes=30),  # SLA de 30 minutos
        environment={
            'DATABASE_URL': f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@db_postgres:5432/{os.getenv('POSTGRES_DB')}",
            'NASA_API_KEY': os.getenv('NASA_API_KEY'),
        }
    )

    # ----------------------------
    # CONFIGURAÇÃO COMUM DBT
    # ----------------------------
    dbt_common_config = {
        'image': 'spacex_dbt_custom:latest',
        'api_version': DOCKER_API_VERSION,
        'auto_remove': True,
        'docker_url': 'unix://var/run/docker.sock',
        'network_mode': NETWORK_NAME,
        'mount_tmp_dir': False,
        'working_dir': '/usr/app',
        'mounts': [Mount(source=DBT_PROJECT_PATH, target='/usr/app', type='bind')],
        'environment': {
            'DBT_PROFILES_DIR': '/usr/app',
            'DBT_TARGET': 'docker',
            'POSTGRES_USER': os.getenv('POSTGRES_USER', 'admin'),
            'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD', 'PASSWORD'),
            'POSTGRES_DB': os.getenv('POSTGRES_DB', 'spacex_db'),
            'POSTGRES_HOST': os.getenv('POSTGRES_HOST', 'db_postgres'),
        },
        'force_pull': False,
    }

    # ----------------------------
    # TASKS DBT
    # ----------------------------
    dbt_deps = DockerOperator(
        task_id='dbt_deps',
        command='dbt deps --target docker',
        **dbt_common_config
    )

    dbt_freshness = DockerOperator(
        task_id='dbt_freshness',
        command='dbt source freshness --target docker',
        **dbt_common_config
    )

    dbt_run = DockerOperator(
        task_id='dbt_run',
        command='dbt run --target docker',
        **dbt_common_config
    )

    dbt_test = DockerOperator(
        task_id='dbt_test',
        command='dbt test --target docker --warn-error',  # falha DAG se algum teste falhar
        **dbt_common_config
    )

    dbt_docs = DockerOperator(
        task_id='dbt_docs',
        command='dbt test --target docker --warn-error',
        **dbt_common_config
    )

    # ----------------------------
    # SEQUÊNCIA DE EXECUÇÃO
    # ----------------------------
    ingest_data >> dbt_deps >> dbt_freshness >> dbt_run >> dbt_test >> dbt_docs