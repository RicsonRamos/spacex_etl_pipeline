import argparse
import structlog
from src.flows.flows import spacex_main_pipeline


class PipelineExecutor:
    def __init__(self, incremental: bool):
        """
        Inicializa o executor de pipeline com o modo de execução.

        :param incremental: Define se o pipeline deve ser executado de forma incremental (novos dados) ou completo (full).
        """
        self.incremental = incremental
        self.logger = structlog.get_logger()

    def start_pipeline(self):
        """
        Inicia o SpaceX Medallion Pipeline, seja incremental ou completo.
        """
        self._log_pipeline_start()
        try:
            spacex_main_pipeline(incremental=self.incremental)
            self._log_pipeline_success()
        except Exception as e:
            self._log_pipeline_failure(e)

    def _log_pipeline_start(self):
        """
        Registra o início do pipeline com o modo selecionado.
        """
        mode = "incremental" if self.incremental else "full"
        self.logger.info("Iniciando SpaceX Medallion Pipeline", mode=mode)

    def _log_pipeline_success(self):
        """
        Registra o sucesso do pipeline.
        """
        self.logger.info("Pipeline finalizado com sucesso 🚀")

    def _log_pipeline_failure(self, error):
        """
        Registra a falha do pipeline.
        """
        self.logger.error("Falha catastrófica no ponto de entrada", error=str(error))


class CLI:
    def __init__(self):
        """
        Inicializa a CLI com a configuração dos parâmetros de execução.
        """
        self.parser = argparse.ArgumentParser(description="SpaceX Medallion Pipeline CLI")
        self.parser.add_argument(
            "--incremental",
            action="store_true",
            default=False,
            help="Executa o pipeline processando apenas novos registros (delta load)",
        )

    def parse_args(self):
        """
        Faz o parse dos argumentos da linha de comando.

        :return: Argumentos parsed (incremental).
        """
        return self.parser.parse_args()


def main():
    """
    Função principal que orquestra a execução do pipeline a partir da CLI.
    """
    # Parse dos argumentos da linha de comando
    cli = CLI()
    args = cli.parse_args()

    # Inicializa o executor do pipeline com o modo desejado
    pipeline_executor = PipelineExecutor(incremental=args.incremental)

    # Inicia a execução do pipeline
    pipeline_executor.start_pipeline()


if __name__ == "__main__":
    main()