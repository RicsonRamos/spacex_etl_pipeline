from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import os
import sys
import logging
import json

# ----------------------------
# LOGGER JSON
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
        "log_url": ti.log_url,
    }
    logger.error(json.dumps(log_msg))
    
    if alert_email:
        try:
            subject = f"[FALHA] DAG {ti.dag_id} | Task {ti.task_id}"
            body = f"""
            <h3>Falha na Pipeline SpaceX</h3>
            <ul>
                <li><strong>DAG:</strong> {ti.dag_id}</li>
                <li><strong>Task:</strong> {ti.task_id}</li>
                <li><strong>Log:</strong> <a href='{ti.log_url}'>Ver logs</a></li>
            </ul>
            """
            send_email(to=alert_email, subject=subject, html_content=body)
        except Exception as e:
            logger.error(f"Erro ao enviar email: {str(e)}")

# ----------------------------
# CONFIGURAÇÃO
# ----------------------------
DOCKER_API_VERSION = '1.44'
NETWORK_NAME = 'spacex_etl_pipeline_default'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 3,
    'retry_delay': timedelta(seconds=60),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'on_failure_callback': on_failure_callback,
}

# ----------------------------
# FUNÇÃO DE VALIDAÇÃO
# ----------------------------
def validate_environment(**context):
    """Valida variáveis de ambiente necessárias"""
    required_vars = [
        'POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_DB',
        'NASA_API_KEY'
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        raise ValueError(f"Variáveis de ambiente faltando: {missing}")
    
    logger.info("Validação de ambiente concluída com sucesso")
    return True

# ----------------------------
# DAG PRINCIPAL
# ----------------------------
with DAG(
    'spacex_full_pipeline',
    default_args=default_args,
    description='Pipeline ETL SpaceX com DBT',
    schedule='@daily',
    catchup=False,
    tags=['spacex', 'dbt', 'etl', 'space'],
    max_active_runs=1,
) as dag:

    # ----------------------------
    # TASK 0: Validação de Ambiente
    # ----------------------------
    validate_env = PythonOperator(
        task_id='validate_environment',
        python_callable=validate_environment,
    )

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
        force_pull=False,
        environment={
            'DATABASE_URL': (
                f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
                f"@db_postgres:5432/{os.getenv('POSTGRES_DB')}"
            ),
            'NASA_API_KEY': os.getenv('NASA_API_KEY'),
            'SPACEX_API_URL': os.getenv('SPACEX_API_URL', 'https://api.spacexdata.com/v4/launches'),
        },
        sla=timedelta(minutes=30),
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
        'force_pull': False,
        'working_dir': '/usr/app',
        # IMPORTANTE: Sobrescreve o entrypoint da imagem base
        'entrypoint': ['dbt'],
        'environment': {
            'DBT_PROFILES_DIR': '/usr/app',
            'DBT_TARGET': 'docker',
            'POSTGRES_USER': os.getenv('POSTGRES_USER', 'admin'),
            'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD', 'PASSWORD'),
            'POSTGRES_DB': os.getenv('POSTGRES_DB', 'spacex_db'),
            'POSTGRES_HOST': os.getenv('POSTGRES_HOST', 'db_postgres'),
        },
    }

    # ----------------------------
    # TASK 2: DBT Deps
    # ----------------------------
    dbt_deps = DockerOperator(
        task_id='dbt_deps',
        command='deps --target docker',  # Sem "dbt" no início
        **dbt_common_config
    )

    # ----------------------------
    # TASK 3: DBT Source Freshness
    # ----------------------------
    dbt_freshness = DockerOperator(
        task_id='dbt_freshness',
        command='source freshness --target docker',  # Sem "dbt" no início
        **dbt_common_config
    )

    # ----------------------------
    # TASK 4: DBT Run
    # ----------------------------
    dbt_run = DockerOperator(
        task_id='dbt_run',
        command='run --target docker',  # Sem "dbt" no início
        **dbt_common_config
    )

    # ----------------------------
    # TASK 5: DBT Test
    # ----------------------------
    dbt_test = DockerOperator(
        task_id='dbt_test',
        command='test --target docker', 
        **dbt_common_config
    )

    # ----------------------------
    # TASK 6: DBT Docs Generate
    # ----------------------------
    dbt_docs = DockerOperator(
        task_id='dbt_docs_generate',
        command='docs generate --target docker',  # Sem "dbt" no início
        **dbt_common_config
    )

    # ----------------------------
    # PIPELINE COMPLETA
    # ----------------------------
    validate_env >> ingest_data >> dbt_deps >> dbt_freshness >> dbt_run >> dbt_test >> dbt_docs