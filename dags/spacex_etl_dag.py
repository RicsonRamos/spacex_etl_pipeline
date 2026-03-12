from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta
import os

# RIGOR: Captura o caminho absoluto injetado pelo Compose. Fim do Hardcoding.
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
    schedule='@daily',
    catchup=False,
    tags=['finance', 'spacex', 'nasa']
) as dag:

    # TASK 1: Ingestão (Bronze)
    ingest_data = DockerOperator(
        task_id='ingest_data',
        image='spacex_etl_pipeline-ingestion_engine',
        api_version=DOCKER_API_VERSION,
        auto_remove='success',
        docker_url='unix://var/run/docker.sock',
        network_mode='spacex_etl_pipeline_default',
        mount_tmp_dir=False,
        environment={
            'DOCKER_API_VERSION': DOCKER_API_VERSION,
            'NASA_API_KEY': os.getenv('NASA_API_KEY')
        }
    )

   # Configuração centralizada atualizada
    dbt_common_config = {
        'image': 'ghcr.io/dbt-labs/dbt-postgres:1.5.0',
        'api_version': DOCKER_API_VERSION,
        'auto_remove': 'success',
        'docker_url': 'unix://var/run/docker.sock',
        'network_mode': 'spacex_etl_pipeline_default',
        'mount_tmp_dir': False,
        'working_dir': '/usr/app',
        # O PULO DO GATO: Sobrescrevemos o Entrypoint para aceitar shell
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

    # Agora os comandos devem ser apenas a string final, sem repetir 'sh -c'
    dbt_freshness = DockerOperator(
        task_id='dbt_freshness',
        command='dbt source freshness --profiles-dir . --target docker',
        **dbt_common_config
    )

    dbt_run = DockerOperator(
        task_id='dbt_run',
        # dbt deps garante que os pacotes existam antes do run
        command='dbt deps && dbt run --profiles-dir . --target docker', 
        **dbt_common_config
    )

    dbt_test = DockerOperator(
        task_id='dbt_test',
        # O dbt_test também precisa dos pacotes para compilar os testes corretamente
        command='dbt deps && dbt test --profiles-dir . --target docker',
        **dbt_common_config
    )

    ingest_data >> dbt_freshness >> dbt_run >> dbt_test