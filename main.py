from src.config.config import settings  # <--- LINHA FALTANTE
from src.extract.extractor import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.loader import SpaceXLoader
from src.utils.logger import setup_logger

logger = setup_logger("main")

def run_pipeline():
    # Instanciação dos componentes de produção
    extractor = SpaceXExtractor()
    transformer = SpaceXTransformer()
    loader = SpaceXLoader()

    # RIGOR: Acessando explicitamente o dicionário carregado pelo singleton settings
    if not settings.API_ENDPOINTS:
        logger.error("Nenhum endpoint definido no settings.yaml!")
        return

    for endpoint, path in settings.API_ENDPOINTS.items():
        try:
            logger.info(f"--- Iniciando processamento: {endpoint} ---")
            
            # 1. Extração
            raw_data = extractor.extract(path)
            if not raw_data:
                logger.warning(f"Sem dados extraídos para {endpoint}. Pulando...")
                continue
            
            # 2. Transformação (Filtro pelo Núcleo Duro)
            clean_df = transformer.transform(endpoint, raw_data)
            
            # 3. Carga (Upsert Idempotente)
            loader.upsert(endpoint, clean_df)
            
            logger.info(f"SUCESSO: {endpoint.upper()} finalizado.")
            
        except Exception as e:
            logger.error(f"FALHA CRÍTICA em {endpoint}: {str(e)}", exc_info=True)

if __name__ == "__main__":
    run_pipeline()