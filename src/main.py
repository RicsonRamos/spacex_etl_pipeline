import argparse
import structlog

# Importa o fluxo principal do Prefect
from src.flows.flows import spacex_main_pipeline

# Configuração básica de logs estruturados
logger = structlog.get_logger()

def main(incremental: bool = False):
    """
    Ponto de entrada que conecta a CLI ao Flow do Prefect.

    :param incremental: Define se o pipeline será executado de forma incremental (somente novos dados) ou completo (full).
    """
    logger.info(
        "Iniciando SpaceX Medallion Pipeline",
        mode="incremental" if incremental else "full",
    )

    try:
        # Chama o Flow 'spacex_main_pipeline' com o argumento 'incremental'
        spacex_main_pipeline(incremental=incremental)

        logger.info("Pipeline finalizado com sucesso 🚀")

    except Exception as e:
        # Loga erro caso ocorra alguma falha no pipeline
        logger.error("Falha catastrófica no ponto de entrada", error=str(e))
        raise

if __name__ == "__main__":
    # Configuração da CLI para o pipeline com argparse
    parser = argparse.ArgumentParser(description="SpaceX Medallion Pipeline CLI")
    parser.add_argument(
        "--incremental",
        action="store_true",  # Faz com que a flag --incremental seja tratada como True
        default=False,        # Caso não passe a flag, a execução será completa
        help="Executa o pipeline processando apenas novos registros (delta load)",
    )
    args = parser.parse_args()

    # Chama a função principal passando o modo incremental
    main(incremental=args.incremental)