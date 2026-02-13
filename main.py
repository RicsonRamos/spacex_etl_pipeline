from src.config.config import settings
from src.extract.extract import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.load import SpaceXLoader
from src.utils.logger import setup_logger

def main():
    # 1. Configuração do Logger Central
    logger = setup_logger("main_pipeline")
    logger.info("Iniciando Pipeline ETL SpaceX (Arquitetura Híbrida)")

    try:
        # 2. Inicialização dos Módulos (Todos agora buscam config sozinhos)
        extractor = SpaceXExtractor()
        transformer = SpaceXTransformer()
        loader = SpaceXLoader()

        # 3. EXTRAÇÃO (Massa)
        # O fetch_all percorre o manifesto e traz um dict {nome: dados_brutos}
        raw_data_map = extractor.fetch_all()
        
        if not raw_data_map:
            logger.error("Nenhum dado extraído. Verifique a conexão ou o manifesto.")
            return

        # 4. CICLO ETL DINÂMICO
        for endpoint_name, data in raw_data_map.items():
            logger.info(f"--- Processando Unidade: {endpoint_name.upper()} ---")
            
            try:
                # A: Transformação (Usa o mapeamento do JSON)
                df_processed = transformer.transform(endpoint_name, data)
                
                if df_processed.empty:
                    logger.warning(f" {endpoint_name}: DataFrame vazio após transformação. Pulando carga.")
                    continue

                # B: Carga (Decide automaticamente entre Upsert ou Genérica)
                loader.load(endpoint_name, df_processed)
                
            except Exception as e:
                # Rigor: Um erro em um endpoint não deve derrubar o pipeline inteiro
                logger.error(f"Falha no ciclo do endpoint '{endpoint_name}': {e}")
                continue

        logger.info("Pipeline finalizado com Sucesso Absoluto.")

    except Exception as e:
        logger.critical(f"Falha catastrófica no motor do pipeline: {e}")

if __name__ == "__main__":
    main()