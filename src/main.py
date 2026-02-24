import structlog
import argparse

from src.utils.logging import setup_logging
from src.flows.etl_flow import spacex_main_pipeline


def main(incremental: bool = False):
    """
    Ponto de entrada que conecta a CLI ao Flow.
    """

    setup_logging()  # <-- inicializa logging aqui, não no import
    logger = structlog.get_logger()

    logger.info(
        "Iniciando SpaceX Medallion Pipeline",
        mode="incremental" if incremental else "full",
    )

    try:
        spacex_main_pipeline(incremental=incremental)
        logger.info("Pipeline finalizado com sucesso 🚀")

    except Exception as e:
        logger.error("Falha catastrófica no ponto de entrada", error=str(e))
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="SpaceX Medallion Pipeline CLI"
    )

    parser.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help="Executa o pipeline processando apenas novos registros (delta load)",
    )

    args = parser.parse_args()
    main(incremental=args.incremental)