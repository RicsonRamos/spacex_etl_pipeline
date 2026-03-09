import os
import datetime
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

    loader = PostgresLoader()

    for name, config in ENDPOINTS_CONFIG.items():
        try:
            logger.info(f"Processando endpoint: {name}")

           
            extractor = APIExtractor(
                endpoint_name=name,
                url=config["url"],
                params=config.get("params"),
                json_path=config.get("json_path")
            )

            raw_data = extractor.extract()

            if raw_data.empty:
                logger.warning(f"Extração vazia para {name}")
                continue

            
            raw_data["source_endpoint"] = name
            raw_data["data_layer"] = config.get("layer", "bronze") 
            raw_data["ingestion_timestamp"] = datetime.datetime.utcnow()

            loader.load_bronze(
                raw_data,
                table_name=name
            )

            logger.info(f"{name} carregado na camada bronze")

        except Exception as e:
            logger.error(f"Erro no pipeline {name}: {str(e)}")
            continue

    logger.info("--- Motor de Ingestão finalizado ---")

if __name__ == "__main__":
    run_ingestion_engine()