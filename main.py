import os
import datetime
import pandas as pd
from dotenv import load_dotenv
from config import endpoints
from config.endpoints import get_endpoints_config
from src.extractors.concrete_extractors import APIExtractor
from src.loaders.postgres_loader import PostgresLoader
from src.utils.logger import get_logger
from src.utils.notifications import AlertSystem


load_dotenv()
logger = get_logger("MainOrchestrator")

def preflight_check(df: pd.DataFrame, endpoint_name: str) -> bool:
    """
    Rigor: Valida se o DataFrame atende aos critérios mínimos de qualidade.
    """
    if df.empty:
        logger.warning(f"Check Falhou: {endpoint_name} está vazio.")
        return False
    
    # Exemplo de Validação de Contrato (Campos Críticos)
    critical_columns = {
        "spacex_launches": ["id", "flight_number", "date_utc"],
        "nasa_solar_events": ["activityID", "eventTime"]
    }
    
    if endpoint_name in critical_columns:
        missing = [col for col in critical_columns[endpoint_name] if col not in df.columns]
        if missing:
            logger.error(f"Contrato violado em {endpoint_name}. Colunas ausentes: {missing}")
            return False
            
    # Validação de Nulos em IDs
    if "id" in df.columns and df["id"].isnull().any():
        logger.warning(f"Detectados IDs nulos em {endpoint_name}. Procedendo com cautela.")
        
    return True

def run_ingestion_engine():
    logger.info("--- Iniciando Motor de Ingestão Enterprise (ELT) ---")
    loader = PostgresLoader()
    alert_maneger = AlertSystem()

    endpoints = get_endpoints_config()
    for name, config in endpoints.items():
        try:
            logger.info(f"Processando endpoint: {name}")
            
            extractor = APIExtractor(
                endpoint_name=name,
                url=config["url"],
                params=config.get("params"),
                json_path=config.get("json_path")
            )

            raw_data = extractor.extract()

            # PRE-FLIGHT CHECK
            if not preflight_check(raw_data, name):
                msg = f"Falha na qualidade dos dados para {name}. Verifique os logs para detalhes."
                alert_maneger.notify_critical_failure(name, msg, serverity="WARNING")
                logger.error(f"Abortando ingestão de {name} por falha na qualidade pré-vôo.")
                continue
            

            raw_data["source_endpoint"] = name
            raw_data["data_layer"] = config.get("layer", "bronze") 
            raw_data["ingestion_timestamp"] = datetime.datetime.utcnow()

            loader.load_bronze(raw_data, table_name=name)
            logger.info(f"{name} carregado na camada bronze")

        except Exception as e:
            alert_maneger.notify_critical_failure(name, str(e))
            logger.error(f"Erro no pipeline {name}: {str(e)}")
            continue

    logger.info("--- Motor de Ingestão finalizado ---")

if __name__ == "__main__":
    run_ingestion_engine()