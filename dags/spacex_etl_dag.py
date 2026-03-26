from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import os
import logging

logger = logging.getLogger("airflow.task")

# ----------------------------
# CALLBACK DE FALHA COM ALERTA
# ----------------------------
def on_failure_callback(context):
    ti = context.get('task_instance')
    
    alert_email = os.getenv('ALERT_EMAIL')
    if not alert_email:
        logger.warning("ALERT_EMAIL não definido. Falha não será enviada por email.")
        return

    subject = f"DAG {ti.dag_id} | TASK {ti.task_id} FAILED"
    body = f"""
    DAG: {ti.dag_id}<br>
    Task: {ti.task_id}<br>
    Run ID: {ti.run_id}<br>
    Log URL: <a href='{ti.log_url}'>Clique aqui para logs</a>
    """
    logger.error(f'TASK FAIL: {ti.task_id} | DAG: {ti.dag_id} | RUN: {ti.run_id}')
    
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
        command='dbt test --target docker --warn-error',
        **dbt_common_config
    )

    dbt_docs = DockerOperator(
        task_id='dbt_docs',
        command='dbt docs generate --target docker',
        **dbt_common_config
    )

    # ----------------------------
    # SEQUÊNCIA DE EXECUÇÃO
    # ----------------------------
    ingest_data >> dbt_deps >> dbt_freshness >> dbt_run >> dbt_test >> dbt_docs