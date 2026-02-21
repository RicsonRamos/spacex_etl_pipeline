import subprocess
from prefect import task
import structlog

logger = structlog.get_logger()

@task(retries=2, name="Run DBT Models")
def run_dbt():
    cmd = ["dbt", "build", "--project-dir", "/app/dbt"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error("DBT falhou", stderr=result.stderr)
        raise RuntimeError("DBT Build Failure")
