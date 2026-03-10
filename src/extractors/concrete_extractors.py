import requests
import pandas as pd
from src.interfaces.extractor_interface import DataExtractor
from src.utils.logger import get_logger

logger = get_logger(__name__)

class APIExtractor(DataExtractor):
    def __init__(
            self, endpoint_name: str, 
            url: str, params: dict = None, 
            headers: dict = None,
            json_path: str = None
    ):
        self.endpoint_name = endpoint_name
        self.url = url
        self.params = params
        self.headers = headers
        self.json_path = json_path

    def extract(self) -> pd.DataFrame:
        logger.info(f"Iniciando extração do endpoint: {self.endpoint_name}")
        try:
            # Add suport a param (NASA api key) e headers
            response = requests.get(
                self.url, 
                params=self.params, 
                headers=self.headers, 
                timeout=20
            )
            response.raise_for_status()
            
            data = response.json()

            # Lógicca para acessar chaves específicas no JSON, caso json_path seja fornecido
            if self.json_path:
                for key in self.json_path.split('.'):
                    data = data.get(key, {})

            df = pd.json_normalize(data)

            if df.empty:
                logger.warning(f"Nenhum dado encontrado no endpoint {self.endpoint_name}.")
            
            logger.info(f"Extração concluída: {len(df)} registros recuperados de {self.endpoint_name}.")
            return df
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"Falha HTTP no endpoint {self.endpoint_name}: {e}")
            raise
        except Exception as e:
            logger.critical(f"Erro inesperado na extração de {self.endpoint_name}: {e}")
            raise