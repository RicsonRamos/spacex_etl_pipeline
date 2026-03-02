import os
import pandas as pd
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger(__name__)

class LocalLoader:
    """
    Responsável pela persistência de dados seguindo a arquitetura Medallion.
    Garante idempotência através de nomes de arquivos fixos (latest).
    """

    @staticmethod
    def _get_base_path() -> str:
        # Rigor: Garante que o ponto de partida seja sempre previsível
        return os.getenv("BASE_DATA_PATH", "./data")

    @classmethod
    def _save(cls, df: pd.DataFrame, layer: str, dataset_name: str, file_name: str) -> str:
        """
        Motor de salvamento privado para evitar repetição de lógica.
        """
        target_dir = os.path.join(cls._get_base_path(), layer, dataset_name)
        os.makedirs(target_dir, exist_ok=True)
        
        file_path = os.path.join(target_dir, file_name)
        
        try:
            # Rigor: Gravação atômica (ou o arquivo grava inteiro ou falha)
            df.to_parquet(file_path, index=False)
            logger.info(f"Camada {layer.upper()} persistida com sucesso em: {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Erro crítico ao persistir no disco (Camada {layer}): {e}")
            raise

    @classmethod
    def save_raw(cls, df: pd.DataFrame, dataset_name: str) -> str:
        """
        Persiste o dado bruto (Bronze) e injeta metadados de extração.
        """
        # Rigor Analítico: O dado bruto DEVE ter o timestamp de quando saiu da API
        df_copy = df.copy()
        df_copy['extracted_at'] = datetime.now().isoformat()
        
        return cls._save(df_copy, "bronze", dataset_name, "raw_latest.parquet")

    @classmethod
    def save_processed(cls, df: pd.DataFrame, dataset_name: str) -> str:
        """
        Persiste o dado limpo (Silver).
        """
        # Rigor: Na Silver não reinjetamos extracted_at se ele já vier da Bronze
        return cls._save(df, "silver", dataset_name, "silver_latest.parquet")