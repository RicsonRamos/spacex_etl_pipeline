import logging
from src.config.config import settings
from src.extract.extractor import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.loader import SpaceXLoader
from src.utils.logger import setup_logger
from src.utils.metrics import StepTimer

logger = setup_logger("main")

# RIGOR: Ordem de dependência para garantir integridade referencial no DB
DEPENDENCY_ORDER = ['rockets', 'launchpads', 'landpads', 'payloads', 'launches']

def run_pipeline():
    extractor = SpaceXExtractor()
    transformer = SpaceXTransformer()
    loader = SpaceXLoader()
    
    # Valida quais endpoints do YAML estão na nossa ordem de dependência
    active_endpoints = [e for e in DEPENDENCY_ORDER if e in settings.API_ENDPOINTS]

    for endpoint in active_endpoints:
        logger.info(f"=== INICIANDO: {endpoint.upper()} ===")
        timer = StepTimer()

        try:
            # EXTRAÇÃO (Nota: alterado para .extract conforme sua implementação)
            with timer.measure("extract"):
                raw_data = extractor.extract(endpoint)

            if not raw_data:
                logger.warning(f"Sem dados extraídos para {endpoint}. Pulando...")
                continue

            # TRANSFORMAÇÃO
            with timer.measure("transform"):
                clean_df = transformer.transform(endpoint, raw_data)

            # CARREGAMENTO
            if not clean_df.empty:
                with timer.measure("load"):
                    loader.upsert(endpoint, clean_df)

                # MÉTRICAS DE EXECUÇÃO
                total_time = timer.total_time()
                records = len(clean_df)
                throughput = timer.throughput(records)

                logger.info(
                    f"[METRICS] {endpoint.upper()} | "
                    f"E: {timer.metrics.get('extract', 0)}s | "
                    f"T: {timer.metrics.get('transform', 0)}s | "
                    f"L: {timer.metrics.get('load', 0)}s | "
                    f"Vazão: {throughput} rows/sec"
                )
            else:
                logger.warning(f"DataFrame vazio para {endpoint} após transformação.")

        except Exception as e:
            logger.error(f"FALHA NO ENDPOINT {endpoint}: {str(e)}", exc_info=True)

    logger.info("=== PIPELINE FINALIZADO ===")

if __name__ == "__main__":
    run_pipeline()