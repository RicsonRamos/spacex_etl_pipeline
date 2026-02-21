import structlog
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

# Importação dos componentes modulares corrigidos
from src.extract.spacex_api import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.loader import PostgresLoader
from src.utils.monitoring import (
    EXTRACT_COUNT, 
    SILVER_COUNT, 
    start_metrics_server, 
    slack_notify
)

# Logger estruturado para rastreabilidade
logger = structlog.get_logger()

# =====================================================
# TASK: PROCESSAMENTO ATÓMICO POR ENTIDADE
# =====================================================
@task(
    retries=3, 
    retry_delay_seconds=30, 
    name="Process SpaceX Entity"
)
def process_entity_task(endpoint: str):
    """
    Coordena o ciclo de vida de uma entidade específica (Ex: launches).
    O isolamento em task permite que se uma falhar, as outras continuem.
    """
    # Inicialização local para evitar problemas de concorrência em workers
    extractor = SpaceXExtractor()
    transformer = SpaceXTransformer()
    loader = PostgresLoader()

    try:
        # 1. IDENTIFICAÇÃO DE ESTADO (Incremental)
        # Busca a última data ingerida na tabela silver correspondente
        table_name = f"silver_{endpoint}"
        last_date = loader.get_last_ingested(table_name)
        
        logger.info("Iniciando processamento", endpoint=endpoint, last_ingested=last_date)

        # 2. EXTRAÇÃO
        raw_data = extractor.fetch(endpoint, incremental=True, last_ingested=last_date)
        EXTRACT_COUNT.labels(endpoint).inc(len(raw_data))

        if not raw_data:
            logger.info("Nenhum dado novo encontrado", endpoint=endpoint)
            return {"endpoint": endpoint, "status": "no_new_data", "count": 0}

        # 3. CARGA BRONZE (Audit Trail)
        loader.load_bronze(raw_data, endpoint, source="spacex_api")

        # 4. TRANSFORMAÇÃO (Lógica de Negócio + Polars)
        df = transformer.transform(endpoint, raw_data)

        # 5. CARGA SILVER (Upsert Canónico)
        rows_upserted = loader.upsert_silver(df, endpoint)
        SILVER_COUNT.labels(endpoint).inc(rows_upserted)

        # 6. REFRESH GOLD (Opcional - se houver view definida)
        loader.refresh_gold_view(endpoint)

        return {"endpoint": endpoint, "status": "success", "count": rows_upserted}

    except Exception as e:
        logger.error("Falha no processamento da entidade", endpoint=endpoint, error=str(e))
        slack_notify(f"❌ Erro no Pipeline SpaceX: {endpoint} falhou. Verifique os logs.")
        raise


# =====================================================
# FLOW: ORQUESTRADOR PRINCIPAL (MAESTRO)
# =====================================================
@flow(
    name="SpaceX Enterprise ETL",
    description="Pipeline principal para ingestão e processamento de dados da SpaceX API",
    task_runner=ConcurrentTaskRunner() # Executa entidades em paralelo se houver recursos
)
def spacex_main_pipeline():
    """
    O Maestro: Não executa lógica, apenas coordena as tarefas.
    """
    # Inicia servidor Prometheus para exposição de métricas (Porta 8000)
    start_metrics_server(8000)

    # Definição das entidades a processar (Controle Centralizado)
    entities = ["rockets", "launches", "launchpads"]

    logger.info("Iniciando SpaceX Main Pipeline")

    # Disparo de tarefas assíncronas
    # O Prefect gere a fila e a execução paralela
    for entity in entities:
        process_entity_task.submit(entity)

    logger.info("Pipeline disparado com sucesso")


if __name__ == "__main__":
    # Execução local para testes
    spacex_main_pipeline()
