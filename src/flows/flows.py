import structlog
from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner
from src.flows.tasks import process_entity_task
from src.utils.dbt_tools import run_dbt
from src.utils.monitoring import start_metrics_server

# Configuração básica de logs estruturados
logger = structlog.get_logger()

@flow(name="SpaceX Enterprise ETL", task_runner=ConcurrentTaskRunner())
def spacex_main_pipeline(incremental: bool = False, real_api: bool = False):
    """
    Fluxo principal ETL SpaceX (Rockets → Launches → Gold/dbt)

    :param incremental: Define se o pipeline deve ser executado de forma incremental (somente dados novos).
    :param real_api: Define se o pipeline deve usar a API real ou mock.
    """
    # Inicializa métricas Prometheus
    start_metrics_server(8000)

    # Ingestão de dimensões base (Rockets)
    logger.info("Iniciando ingestão de Rockets")
    rocket_future = process_entity_task.submit("rockets", real_api=real_api, incremental=incremental)
    rocket_future.wait()  # garante que FKs existam antes de launches

    # Ingestão de fatos (Launches)
    logger.info("Iniciando ingestão de Launches")
    launch_future = process_entity_task.submit("launches", real_api=real_api, incremental=incremental)
    launch_future.wait()

    # Camada Gold (transformações analíticas via dbt)
    logger.info("Iniciando transformações analíticas (dbt)")
    run_dbt()


if __name__ == "__main__":
    # Passa o parâmetro incremental via linha de comando ou padrão
    spacex_main_pipeline(incremental=True)  # Ou False dependendo do modo que deseja rodar