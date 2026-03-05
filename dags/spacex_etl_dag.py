from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

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
    catchup=False
) as dag:

    # TASK 1: Ingestão
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

    # TASK 2: Transformação
    dbt_run = DockerOperator(
        task_id='dbt_run',
        image='ghcr.io/dbt-labs/dbt-postgres:1.5.0',
        container_name='airflow_dbt_run',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='spacex_etl_pipeline_default',
        command='run', 
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source='D:/Documentos/VsCode/projetos_git/spacex_etl_pipeline/dbt_spacex', 
                target='/usr/app/dbt', 
                type='bind'
            )
        ],
        working_dir='/usr/app/dbt',
        environment={'DBT_PROFILES_DIR': '/usr/app/dbt'}
    )

    # TASK 3: Teste de Qualidade
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
                source='D:/Documentos/VsCode/projetos_git/spacex_etl_pipeline/dbt_spacex', 
                target='/usr/app/dbt', 
                type='bind'
            )
        ],
        working_dir='/usr/app/dbt',
        environment={'DBT_PROFILES_DIR': '/usr/app/dbt'}
    )

    ingest_data >> dbt_run >> dbt_test