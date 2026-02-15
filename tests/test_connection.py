import sys
import os

# Garante que o Python encontre a pasta src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.logging import logger
from src.config.settings import settings
from src.load.postgres_loader import PostgresLoader
import polars as pl

def test_pipeline_sanity():
    logger.info("Iniciando Teste de Sanidade do Pipeline")

    try:
        # 1. Testar Configurações
        logger.info("Validando configurações...", host=settings.POSTGRES_HOST, db=settings.POSTGRES_DB)
        
        # 2. Testar Conexão com Banco
        db_loader = PostgresLoader()
        
        # 3. Testar Operação de Escrita (Dummy Data)
        test_df = pl.DataFrame({
            "rocket_id": ["test_01"],
            "name": ["Falcon Test"],
            "active": [True]
        })
        
        logger.info("Tentando escrita de teste (Upsert)...")
        db_loader.upsert_dataframe(test_df, "rockets", "rocket_id")
        
        logger.info("TUDO OK: Configurações, Logs e Banco de Dados operacionais.")

    except Exception as e:
        logger.error("FALHA NO TESTE", error=str(e))
        sys.exit(1)

if __name__ == "__main__":
    test_pipeline_sanity()