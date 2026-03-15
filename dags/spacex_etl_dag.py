from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta
import os
import logging

logger = logging.getLogger("airflow.task")

def on_failure_callback(context):
    ti = context.get('task_instance')
    # Ajuste de rigor: Incluindo o erro específico no log se disponível
    logger.error(f'TASK FAIL: {ti.task_id} | DAG: {ti.dag_id} | RUN: {ti.run_id}')

DBT_PROJECT_PATH = os.getenv('DBT_PROJECT_PATH_ON_HOST')
DOCKER_API_VERSION = '1.44'
NETWORK_NAME = 'spacex_etl_pipeline_default'

default_args = {
    'owner': 'airflow',
    'on_failure_callback': on_failure_callback,
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    'spacex_full_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['spacex', 'dbt', 'infrastructure']
) as dag:

    # TASK 1: Ingestão de Dados (Bronze)
    ingest_data = DockerOperator(
        task_id='ingest_data',
        image='spacex_etl_pipeline-ingestion_engine:latest',
        api_version=DOCKER_API_VERSION,
        auto_remove='success',
        docker_url='unix://var/run/docker.sock',
        network_mode=NETWORK_NAME,
        mount_tmp_dir=False,
        environment={
            'DATABASE_URL': f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@spacex_postgres:5432/{os.getenv('POSTGRES_DB')}",
            'NASA_API_KEY': os.getenv('NASA_API_KEY'),
        }
    )

    # Configuração comum para dbt (Image Baking Philosophy)
    dbt_common_config = {
        'image': 'spacex_dbt_custom:latest', 
        'api_version': DOCKER_API_VERSION,
        'auto_remove': 'success',
        'docker_url': 'unix://var/run/docker.sock',
        'network_mode': NETWORK_NAME,
        'mount_tmp_dir': False,
        'working_dir': '/usr/app',
        'mounts': [
            Mount(source=DBT_PROJECT_PATH, target='/usr/app', type='bind')
        ],
        'environment': {
            'DBT_PROFILES_DIR': '/usr/app',
            'POSTGRES_USER': os.getenv('POSTGRES_USER'),
            'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD'),
            'POSTGRES_DB': os.getenv('POSTGRES_DB'),
        }
    }

    # Adicione esta task ANTES da dbt_freshness
    dbt_deps = DockerOperator(
        task_id='dbt_deps',
        command='deps --target docker',
        **dbt_common_config
)

    # TASK 2: dbt Source Freshness (Validação de Ingestão)
    dbt_freshness = DockerOperator(
        task_id='dbt_freshness',
        command='source freshness --target docker', # CORREÇÃO: Removido prefixo 'dbt'
        **dbt_common_config
    )

    # TASK 3: dbt Run (Criação de Silver/Gold)
    dbt_run = DockerOperator(
        task_id='dbt_run',
        command='run --target docker', # CORREÇÃO: Removido prefixo 'dbt'
        **dbt_common_config
    )

    # TASK 4: dbt Test (Qualidade de Dados)
    dbt_test = DockerOperator(
        task_id='dbt_test',
        command='test --target docker', # CORREÇÃO: Removido prefixo 'dbt'
        **dbt_common_config
    )

    ingest_data >> dbt_deps >> dbt_freshness >> dbt_run >> dbt_test