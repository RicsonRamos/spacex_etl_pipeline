import os
import pandas as pd
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger(__name__)

class LocalLoader:
    @staticmethod
    def save_raw(df: pd.DataFrame, dataset_name: str, base_path: str = "data/raw"):
        """
        Rigor: Agora aceita o nome do dataset para organizar subpastas.
        """
        # Criar subpasta para cada dataset (Organização de Data Lake)
        target_dir = os.path.join(base_path, dataset_name)
        os.makedirs(target_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"{dataset_name}_{timestamp}.parquet"
        full_path = os.path.join(target_dir, filename)
        
        try:
            df.to_parquet(full_path, index=False)
            logger.info(f"Arquivo Raw persistido: {full_path}")
            return full_path
        except Exception as e:
            logger.error(f"Erro ao salvar arquivo local: {e}")
            raise