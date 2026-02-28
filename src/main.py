import argparse
import structlog
from src.flows.flows import spacex_main_pipeline
from alembic.config import Config
from alembic import command
import os
import time

class PipelineExecutor:
    def __init__(self, incremental: bool):
        self.incremental = incremental
        self.logger = structlog.get_logger()

    def start_pipeline(self):
        self._log_pipeline_start()
        try:
            self._run_alembic_upgrade()
            spacex_main_pipeline(incremental=self.incremental)
            self._log_pipeline_success()
        except Exception as e:
            self._log_pipeline_failure(e)

    def _run_alembic_upgrade(self):
        """
        Executa Alembic upgrade head antes do pipeline.
        """
        self.logger.info("Executando Alembic upgrade para sincronizar o banco...")
        alembic_cfg = Config(os.path.join(os.path.dirname(__file__), "../alembic.ini"))
        command.upgrade(alembic_cfg, "head")
        self.logger.info("Banco atualizado com Alembic ✅")

    def _log_pipeline_start(self):
        mode = "incremental" if self.incremental else "full"
        self.logger.info("Iniciando SpaceX Medallion Pipeline", mode=mode)

    def _log_pipeline_success(self):
        self.logger.info("Pipeline finalizado com sucesso 🚀")

    def _log_pipeline_failure(self, error):
        self.logger.error("Falha catastrófica no ponto de entrada", error=str(error))

class CLI:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description="SpaceX Medallion Pipeline CLI")
        self.parser.add_argument(
            "--incremental",
            action="store_true",
            default=False,
            help="Executa o pipeline processando apenas novos registros (delta load)",
        )

    def parse_args(self):
        return self.parser.parse_args()

def main():
    cli = CLI()
    args = cli.parse_args()
    executor = PipelineExecutor(incremental=args.incremental)
    executor.start_pipeline()

if __name__ == "__main__":
    main()