import os
from dotenv import load_dotenv
from config.endpoints import ENDPOINTS_CONFIG
from src.extractors.concrete_extractors import APIExtractor
from src.loaders.postgres_loader import PostgresLoader
from src.utils.logger import get_logger

# Rigor: Carregar env antes de qualquer instanciação que dependa de DB_URL
load_dotenv()

logger = get_logger("MainOrchestrator")

def run_ingestion_engine():
    logger.info("--- Iniciando Motor de Ingestão Enterprise (ELT) ---")
    
    # Instancia o loader apenas uma vez (reutilização de conexão/engine)
    loader = PostgresLoader()
    
    for name, config in ENDPOINTS_CONFIG.items():
        try:
            logger.info(f"Processando endpoint: {name}")
            
            # 1. Extração
            extractor = APIExtractor(endpoint_name=name, url=config['url'])
            raw_data = extractor.extract()
            
            if raw_data.empty:
                logger.warning(f"Extração vazia para {name}. Pulando etapa de carga.")
                continue

            # 2. Carga (Load para camada Bronze/Raw no Postgres)
            # Rigor: No modelo ELT, o Python não limpa o dado, apenas persiste.
            loader.load_bronze(raw_data, table_name=name)
            
            logger.info(f"Sucesso: {name} carregado no Banco de Dados.")
            
        except Exception as e:
            logger.error(f"Erro crítico no pipeline '{name}': {str(e)}")
            # Em sistemas enterprise, aqui você dispararia um alerta/sentry
            continue

    logger.info("--- Motor de Ingestão finalizado ---")

if __name__ == "__main__":
    run_ingestion_engine()