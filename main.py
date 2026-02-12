import sys
from src.settings import load_config
from src.extract.extract import SpaceXExtractor
from src.transform.spacex_transformer import SpaceXTransformer
from src.db.database import DatabaseManager
from src.db.schema import SchemaManager
from src.load.load import SpaceXLoader
from src.logger import setup_logger

def main():
    # 1. SETUP INICIAL
    # Inicializa o log principal para rastrear o sucesso/falha da operação
    logger = setup_logger("main_pipeline")
    logger.info("Iniciando Pipeline ETL SpaceX (Rockets + Launches)...")

    try:
        # Carrega configurações (URLs, timeouts, paths)
        config = load_config()
        
        # 2. DATABASE & SCHEMA SETUP
        # Prepara a infraestrutura antes de buscar dados pesados
        db = DatabaseManager()
        schema = SchemaManager(db)
        
        # Criamos as tabelas (usando IF NOT EXISTS dentro do schema)
        # Removido drop_all para preservar dados históricos
        schema.create_tables()

        # 3. EXTRAÇÃO (EXTRACT)
        extractor = SpaceXExtractor(config)
        raw_data = extractor.fetch_all()
        
        # Validação de sanidade: se os dados essenciais falharem, interrompemos aqui
        if not raw_data.get("rockets") or not raw_data.get("launches"):
            raise ValueError("Falha na extração: Um ou mais endpoints retornaram vazios.")

        # 4. TRANSFORMAÇÃO (TRANSFORM)
        transformer = SpaceXTransformer()
        
        logger.info("Transformando dados de Foguetes...")
        # processed_rockets contém: rockets, payloads, images, engines
        processed_rockets = transformer.transform_rockets(raw_data["rockets"])
        
        logger.info("Transformando dados de Lançamentos...")
        processed_launches = transformer.transform_launches(raw_data["launches"])

        # Consolidação dos dados para carga
        all_data = {
            **processed_rockets,
            "launches": processed_launches
        }

        # 5. CARGA (LOAD)
        loader = SpaceXLoader(db.get_engine())
        
        # O loader agora orquestra a ordem de inserção correta para respeitar FKs
        loader.load_tables(all_data)

        logger.info("Pipeline executado com sucesso absoluto.")

    except KeyError as e:
        logger.error(f"Erro de configuração: Chave ausente no YAML/Settings: {e}")
        sys.exit(1)
    except Exception as e:
        # exc_info=True grava o traceback completo no log para debug posterior
        logger.error(f"Falha crítica no pipeline: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()