import argparse

import structlog

from src.flows.etl_flow import spacex_main_pipeline

# Configuração básica de logs estruturados
logger = structlog.get_logger()


def main(incremental: bool = False):
    """
    Ponto de entrada que conecta a CLI ao Flow do Prefect.
    """
    logger.info(
        "Iniciando SpaceX Medallion Pipeline",
        mode="incremental" if incremental else "full",
    )

    try:
        # IMPORTANTE: O Flow 'spacex_main_pipeline' deve aceitar o argumento 'incremental'
        spacex_main_pipeline(incremental=incremental)

        logger.info("Pipeline finalizado com sucesso 🚀")

    except Exception as e:
        # O log de erro aqui é redundante se o Prefect estiver bem configurado,
        # mas útil para depuração local rápida.
        logger.error("Falha catastrófica no ponto de entrada", error=str(e))
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SpaceX Medallion Pipeline CLI")
    parser.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help="Executa o pipeline processando apenas novos registros (delta load)",
    )
    args = parser.parse_args()

    main(incremental=args.incremental)
