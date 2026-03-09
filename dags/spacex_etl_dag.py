from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta
import os

# Rigor: Centralizando caminhos para evitar hardcoding
DBT_PROJECT_PATH_INSIDE_AIRFLOW = '/opt/airflow/dbt'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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
        container_name='airflow_ingestion_run',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='spacex_etl_pipeline_default',
        mount_tmp_dir=False
    )

    # TASK 2: dbt Deps + Run (Silver & Gold)
    # Rigor: dbt run sem dbt deps em ambiente efêmero resulta em erro de compilação.
    dbt_run = DockerOperator(
        task_id='dbt_run',
        image='ghcr.io/dbt-labs/dbt-postgres:1.5.0',
        container_name='airflow_dbt_run',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='spacex_etl_pipeline_default',
        # Executamos deps e run na mesma task para garantir consistência
        command='/bin/bash -c "dbt deps && dbt run"', 
        mount_tmp_dir=False,
        mounts=[
            Mount(
                # O segredo: o Airflow pega o código do volume que ele mesmo já tem montado
                source='spacex_etl_pipeline_db_postgres_data', # Nome do volume ou caminho relativo mapeado no compose
                target='/usr/app/dbt', 
                type='volume' 
            )
        ],
        working_dir='/usr/app/dbt',
        environment={
            'DBT_PROFILES_DIR': '/usr/app/dbt',
            'DB_HOST': 'spacex_postgres' # Garante que o container dbt ache o banco
        }
    )

    # TASK 3: dbt Test (Qualidade)
    dbt_test = DockerOperator(
        task_id='dbt_test',
        image='ghcr.io/dbt-labs/dbt-postgres:1.5.0',
        container_name='airflow_dbt_test',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='spacex_etl_pipeline_default',
        command='test',
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source='spacex_etl_pipeline_db_postgres_data', 
                target='/usr/app/dbt', 
                type='volume'
            )
        ],
        working_dir='/usr/app/dbt',
        environment={
            'DBT_PROFILES_DIR': '/usr/app/dbt',
            'DB_HOST': 'spacex_postgres'
        }
    )

    ingest_data >> dbt_run >> dbt_test