import structlog
from src.flows.etl_flow import spacex_etl_pipeline

logger = structlog.get_logger()

def main(incremental: bool = False):
    """
    Executa o pipeline ETL da SpaceX.

    Parâmetros:
        incremental (bool): Se True, processa apenas novos registros.
    """
    logger.info("Iniciando SpaceX Medallion Pipeline", incremental=incremental)

    try:
        # Chama o pipeline orquestrado pelo Prefect
        spacex_etl_pipeline()
        logger.info("Pipeline finalizado com sucesso 🚀")

    except Exception as e:
        logger.error("Pipeline falhou", error=str(e))
        # Re-raise para que Prefect capture o erro e registre no dashboard
        raise


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Executa o ETL SpaceX Medallion Pipeline")
    parser.add_argument(
        "--incremental", action="store_true", help="Processa apenas novos registros"
    )
    args = parser.parse_args()

    main(incremental=args.incremental)