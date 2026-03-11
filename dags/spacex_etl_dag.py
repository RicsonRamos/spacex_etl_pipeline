from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta
import os

# RIGOR: O Airflow captura o caminho absoluto injetado pelo Docker Compose
# Isso resolve o problema de deploy (funciona em Windows, Linux ou Mac)
DBT_PROJECT_PATH = os.getenv('DBT_PROJECT_PATH_ON_HOST')
DOCKER_API_VERSION = '1.44'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    'spacex_full_pipeline',
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=['finance', 'spacex', 'nasa']
) as dag:

    # TASK 1: Ingestão (Bronze)
    ingest_data = DockerOperator(
        task_id='ingest_data',
        image='spacex_etl_pipeline-ingestion_engine',
        api_version=DOCKER_API_VERSION,
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='spacex_etl_pipeline_default',
        mount_tmp_dir=False,
        environment={
            'DOCKER_API_VERSION': DOCKER_API_VERSION,
            'NASA_API_KEY': os.getenv('NASA_API_KEY')
        }
    )

    # Configuração centralizada para evitar redundância
    dbt_common_config = {
        'image': 'ghcr.io/dbt-labs/dbt-postgres:1.5.0',
        'api_version': DOCKER_API_VERSION,
        'auto_remove': True,
        'docker_url': 'unix://var/run/docker.sock',
        'network_mode': 'spacex_etl_pipeline_default',
        'mount_tmp_dir': False,
        'working_dir': '/usr/app/dbt',
        'mounts': [
            Mount(source=DBT_PROJECT_PATH, target='/usr/app/dbt', type='bind')
        ],
        'environment': {
            'DBT_PROFILES_DIR': '/usr/app/dbt',
            'DB_HOST': 'spacex_postgres',
            'DOCKER_API_VERSION': DOCKER_API_VERSION
        }
    }

    dbt_freshness = DockerOperator(
        task_id='dbt_freshness',
        command='/bin/bash -c "dbt source freshness"',
        **dbt_common_config
    )

    dbt_run = DockerOperator(
        task_id='dbt_run',
        command='/bin/bash -c "dbt deps && dbt run"', 
        **dbt_common_config
    )

    dbt_test = DockerOperator(
        task_id='dbt_test',
        command='/bin/bash -c "dbt test"',
        **dbt_common_config
    )

    ingest_data >> dbt_freshness >> dbt_run >> dbt_test