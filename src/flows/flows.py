import structlog
from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner
from time import perf_counter

from src.utils.monitoring.prometheus_server import PrometheusServer
from src.flows.tasks import process_entity_task
from src.utils.dbt_tools import run_dbt
from src.utils.monitoring.metrics import PipelineMetrics

logger = structlog.get_logger()
metrics = PipelineMetrics()  # Instância única das métricas do pipeline

def _start_observability(enable_metrics: bool) -> None:
    """Inicializa infraestrutura de observabilidade."""
    if not enable_metrics:
        return

    prometheus = PrometheusServer(port=8000)
    prometheus.start()


def _run_entities(incremental: bool, real_api: bool) -> None:
    """Executa processamento das entidades na ordem correta."""
    logger.info("Iniciando ingestão de Rockets")
    rocket_future = process_entity_task.submit(
        "rockets", real_api=real_api, incremental=incremental
    )
    rocket_future.wait()

    logger.info("Iniciando ingestão de Launches")
    launch_future = process_entity_task.submit(
        "launches", real_api=real_api, incremental=incremental
    )
    launch_future.wait()


def _run_gold_layer() -> None:
    """Executa camada analítica (dbt)."""
    logger.info("Executando transformações analíticas (dbt)")
    run_dbt()


@flow(
    name="SpaceX Enterprise ETL",
    task_runner=ConcurrentTaskRunner(),
)
def spacex_main_pipeline(
    incremental: bool = False,
    real_api: bool = False,
    enable_metrics: bool = True,
) -> None:
    """Flow principal do pipeline SpaceX."""
    start_time = perf_counter()
    logger.info("Pipeline iniciado", incremental=incremental, real_api=real_api)

    # Observabilidade
    _start_observability(enable_metrics)

    # Bronze + Silver
    _run_entities(incremental, real_api)

    # Gold
    _run_gold_layer()

    # Métrica de duração
    duration = perf_counter() - start_time
    metrics.flow_time.labels("spacex_main_pipeline").observe(duration)

    logger.info("Pipeline finalizado com sucesso", duration_seconds=round(duration, 2))


if __name__ == "__main__":
    spacex_main_pipeline(incremental=True)