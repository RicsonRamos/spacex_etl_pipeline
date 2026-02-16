from prefect import flow, task
from src.utils.logging import logger
from src.extract.spacex_api import SpaceXExtractor
from src.transform.transformer import SpaceXTransformer
from src.load.loader import PostgresLoader

import os
import sys

print("--- DIAGNÓSTICO DE AMBIENTE ---")
print(f"Diretório atual: {os.getcwd()}")
print(f"Conteúdo de /app: {os.listdir('/app')}")
print(f"PYTHONPATH: {sys.path}")
print("-------------------------------")

#@task(retries=3, retry_delay_seconds=10, name="Extrair Dados SpaceX")
def extract_data():
    """Extrai dados brutos da API da SpaceX com lógica de retry."""
    extractor = SpaceXExtractor()
    data = extractor.fetch_data("launches")
    
    if not data:
        # Erro explícito: evita que o pipeline prossiga com lixo/vazio
        raise ValueError("Falha crítica: O Extrator retornou uma lista vazia.")
    
    return data

#@task(name="Transformar Dados com Polars")
def transform_data(raw_data):
    """Aplica transformações via Polars e garante o esquema de colunas."""
    transformer = SpaceXTransformer()
    df = transformer.transform_launches(raw_data)
    
    # Validação rigorosa de premissa: launch_id deve existir para o Upsert
    if "launch_id" not in df.columns:
        logger.error("Coluna 'launch_id' ausente após transformação")
        raise KeyError("O DataFrame transformado deve conter a coluna 'launch_id'.")
        
    return df

#@task(name="Carregar Dados no Postgres")
def load_data(df):
    """Instancia o loader e executa o UPSERT baseado na chave de negócio."""
    loader = PostgresLoader()
    
    # CORREÇÃO CRÍTICA: 
    # Usamos 'launch_id' como pk_col, pois é o identificador único da SpaceX.
    # O 'id' SERIAL do banco é gerado automaticamente e não deve ser o alvo do conflito.
    loader.upsert_dataframe(df, "launches", "launch_id")

#@flow(name="SpaceX ETL Pipeline Main")
def spacex_etl_flow():
    """Orquestração principal do processo ETL."""
    logger.info("Iniciando Flow de ETL da SpaceX")
    
    # 1. Extração
    raw_launches = extract_data()
    
    # 2. Transformação
    transformed_df = transform_data(raw_launches)
    
    # 3. Carga
    load_data(transformed_df)
    
    logger.info("Flow finalizado com sucesso!")

if __name__ == "__main__":
    # O .fn() executa a função sem tentar conectar ao servidor Prefect
    spacex_etl_flow()