from config.endpoints import ENDPOINTS_CONFIG
from src.extractors.concrete_extractors import APIExtractor
from src.loaders.local_loader import LocalLoader
from src.utils.logger import get_logger
from src.transformers.launches_transformer import LaunchTransformer


logger = get_logger("MainOrchestrator")

def run_ingestion_engine():
    logger.info("Iniciando Motor de Ingestão Multi-Endpoint")
    
    for name, config in ENDPOINTS_CONFIG.items():
        try:
            # 1. Instanciação dinâmica
            extractor = APIExtractor(endpoint_name=name, url=config['url'])
            logger.info(f"Pipeline para {name} iniciado com sucesso.")
            
            # 2. Extração
            raw_data = extractor.extract()
            logger.info(f"Extração para {name} concluida com sucesso.")
            logger.info(f"Quantidade de registros extraídos: {len(raw_data)}")
            
            
            # Salva raw (Bronze)
            LocalLoader.save_raw(raw_data, dataset_name=name)
            logger.info(f"Salva raw para {name}")
            
            # 2.5 Transformação
            df_silver = LaunchTransformer.launches_transform(raw_data)
            logger.info(f"Transformação para {name} concluida com sucesso.")
            
            # Salva Silver
            LocalLoader.save_processed(df_silver, dataset_name=name)
            logger.info(f"Salva processed para {name}")
            
            logger.info(f"Pipeline para {name} finalizado com sucesso.")
            
        except Exception as e:
            logger.error(f"Falha no pipeline do endpoint '{name}': {e}")
            continue

if __name__ == "__main__":
    run_ingestion_engine()