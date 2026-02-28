from prefect import task
from src.application.etl_factory import ETLFactory
from src.utils.monitoring.metrics import PipelineMetrics

metrics = PipelineMetrics()


@task(
    retries=3,
    retry_delay_seconds=30,
    name="Process SpaceX Entity",
)
def process_entity_task(
    entity: str,
    real_api: bool = False,
    incremental: bool = False,
):
    """
    Task responsável apenas por orquestrar a execução
    do ETL de uma entidade.
    """

    service = ETLFactory.create(
        entity=entity,
        incremental=incremental,
        real_api=real_api,
    )

    result = service.run()

    # Atualiza métricas centralizadas
    metrics.inc_extract(entity, 1)
    metrics.inc_silver(entity, 1)

    return result