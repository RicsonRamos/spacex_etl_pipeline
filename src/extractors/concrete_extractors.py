import requests
import pandas as pd
from src.interfaces.extractor_interface import DataExtractor
from src.utils.logger import get_logger

logger = get_logger(__name__)

class APIExtractor(DataExtractor):
    def __init__(self, endpoint_name: str, url: str):
        self.endpoint_name = endpoint_name
        self.url = url

    def extract(self) -> pd.DataFrame:
        logger.info(f"Iniciando extração do endpoint: {self.endpoint_name}")
        try:
            response = requests.get(self.url, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            df = pd.json_normalize(data)
            
            logger.info(f"Extração concluída: {len(df)} registros recuperados de {self.endpoint_name}.")
            return df
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"Falha HTTP no endpoint {self.endpoint_name}: {e}")
            raise
        except Exception as e:
            logger.critical(f"Erro inesperado na extração de {self.endpoint_name}: {e}")
            raise