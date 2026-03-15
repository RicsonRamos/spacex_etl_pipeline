from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta
import os
import logging # RIGOR: Use o logging nativo para evitar ModuleNotFoundError

# Configuração de log padrão do Airflow
logger = logging.getLogger("airflow.task")

def on_failure_callback(context):
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    logger.error(f'Task {task_id} failed on {execution_date}. Logs: {log_url}')

# Captura caminhos e versões
DBT_PROJECT_PATH = os.getenv('DBT_PROJECT_PATH_ON_HOST')
DOCKER_API_VERSION = '1.44'

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
    tags=['finance', 'spacex', 'nasa']
) as dag:

        # TASK 1: Ingestão (Bronze) - CORRIGIDO
    ingest_data = DockerOperator(
        task_id='ingest_data',
        image='spacex_etl_pipeline-ingestion_engine:latest',
        api_version=DOCKER_API_VERSION,
        auto_remove='success',
        docker_url='unix://var/run/docker.sock',
        network_mode='spacex_etl_pipeline_default',
        mount_tmp_dir=False,
        force_pull=False,
        environment={
            'DOCKER_API_VERSION': DOCKER_API_VERSION,
            'NASA_API_KEY': os.getenv('NASA_API_KEY'),
            # CRÍTICO: URL completa que o PostgresLoader espera
            'DATABASE_URL': f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@spacex_postgres:5432/{os.getenv('POSTGRES_DB')}",
        }
    )

   # Configuração centralizada otimizada (Image Baking)
    dbt_common_config = {
        'image': 'spacex_dbt_custom:latest', 
        'api_version': DOCKER_API_VERSION,
        'auto_remove': 'success',
        'docker_url': 'unix://var/run/docker.sock',
        'network_mode': 'spacex_etl_pipeline_default',
        'mount_tmp_dir': False,
        'force_pull': False,  # <-- CRÍTICO: Adicionar aqui
        'working_dir': '/usr/app',
        'entrypoint': ["/bin/sh", "-c"],
        'mounts': [
            Mount(source=DBT_PROJECT_PATH, target='/usr/app', type='bind')
        ],
        'environment': {
            'DBT_PROFILES_DIR': '.',
            'DB_HOST': 'spacex_postgres',
            'POSTGRES_USER': os.getenv('POSTGRES_USER'),
            'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD'),
            'POSTGRES_DB': os.getenv('POSTGRES_DB'),
            'DOCKER_API_VERSION': DOCKER_API_VERSION
        }
    }

    dbt_freshness = DockerOperator(
        task_id='dbt_freshness',
        command='dbt source freshness --profiles-dir . --target docker',
        **dbt_common_config
    )

    dbt_run = DockerOperator(
        task_id='dbt_run',
        # RIGOR: dbt deps removido pois já está na imagem custom
        command='dbt run --profiles-dir . --target docker', 
        **dbt_common_config
    )

    dbt_test = DockerOperator(
        task_id='dbt_test',
        command='dbt test --profiles-dir . --target docker',
        **dbt_common_config
    )

    ingest_data >> dbt_freshness >> dbt_run >> dbt_test