import os
import subprocess

import structlog

logger = structlog.get_logger()


def run_dbt():
    """
    Executa a cadeia de comandos dbt garantindo que o ambiente esteja pronto.
    """
    # Define o diretório do projeto dbt (conforme configurado no Dockerfile/Compose)
    dbt_dir = os.getenv("DBT_PROJECT_DIR", "/app/dbt")

    commands = [
        ["dbt", "deps"],  # Instala pacotes (ex: dbt_utils)
        ["dbt", "debug"],  # Testa conexão com o banco
        ["dbt", "run"],  # Executa os modelos (Staging -> Marts)
        ["dbt", "test"],  # Valida a qualidade dos dados gerados
    ]

    try:
        for cmd in commands:
            logger.info(f"Executando comando dbt: {' '.join(cmd)}")

            result = subprocess.run(cmd, cwd=dbt_dir, capture_output=True, text=True, check=True)

            logger.info(f"Sucesso: {' '.join(cmd)}")
            if result.stdout:
                # Loga apenas o sumário para não poluir o log central
                summary = [
                    line for line in result.stdout.split("\n") if "Actual" in line or "OK" in line
                ]
                logger.debug("dbt output", output=summary)

    except subprocess.CalledProcessError as e:
        logger.error(
            "Falha na execução do dbt",
            command=" ".join(e.cmd),
            error=e.stderr,
            stdout=e.stdout,
        )
        raise RuntimeError(f"Erro no dbt: {e.stderr}") from e
