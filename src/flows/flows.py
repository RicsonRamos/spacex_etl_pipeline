import structlog
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from src.utils.dbt_tools import run_dbt
from src.utils.monitoring import start_metrics_server

logger = structlog.get_logger()


@task
def start_pipeline_monitoring_task(port: int = 8000):
    """Task para iniciar o servidor de métricas Prometheus."""
    logger.info("Iniciando servidor de métricas Prometheus")
    start_metrics_server(port)


@task
def run_dbt_task():
    """Task para executar transformações dbt."""
    logger.info("Iniciando transformações analíticas com dbt")
    run_dbt()


@flow(name="SpaceX Enterprise ETL", task_runner=ConcurrentTaskRunner())
def spacex_main_pipeline(
    incremental: bool = False, 
    real_api: bool = False,
    process_entity_task=None,  # Injeção de dependência para testes
    start_metrics=None,       # Injeção de dependência para testes
    run_dbt=None              # Injeção de dependência para testes
):
    """
    Fluxo principal ETL SpaceX (Rockets → Launches → Gold/dbt)
    
    :param incremental: Define se o pipeline deve ser executado de forma incremental.
    :param real_api: Define se o pipeline deve usar a API real ou mock.
    :param _process_entity_task: Função de processamento (para testes).
    :param start_metrics: Função de métricas (para testes).
    :param run_dbt: Função dbt (para testes).
    """
    # Import tardio permite mockar em testes
    from src.flows.tasks import process_entity_task as _process_task_orig
    
    # Usa injetado ou original
    process_task = process_entity_task or _process_task_orig
    start_metrics_fn = start_metrics or start_pipeline_monitoring_task
    run_dbt_fn = run_dbt or run_dbt_task
    
    # Inicia monitoramento
    start_metrics_fn(8000)
    
    # Ingestão de Rockets (concorrente via submit)
    logger.info("Iniciando ingestão de Rockets")
    rocket_future = process_task.submit("rockets", real_api=real_api, incremental=incremental)
    rocket_future.wait()
    
    # Ingestão de Launches (concorrente via submit)
    logger.info("Iniciando ingestão de Launches")
    launch_future = process_task.submit("launches", real_api=real_api, incremental=incremental)
    launch_future.wait()
    
    # Executa transformações analíticas (dbt)
    logger.info("Iniciando transformações analíticas com dbt")
    run_dbt_fn()


if __name__ == "__main__":
    spacex_main_pipeline(incremental=True)