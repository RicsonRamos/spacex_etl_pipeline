import requests
import pandas as pd
from src.interfaces.extractor_interface import DataExtractor
from src.utils.logger import get_logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = get_logger(__name__)

class APIExtractor(DataExtractor):
    def __init__(self, endpoint_name, url, params=None, headers=None, json_path=None):
        self.endpoint_name = endpoint_name
        self.url = url
        self.params = params
        self.headers = headers
        self.json_path = json_path
        # Configuração de Retry: Rigor contra instabilidade de rede
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    def extract(self) -> pd.DataFrame:
        logger.info(f"Iniciando extração do endpoint: {self.endpoint_name}")
        try:
            response = self.session.get(
                self.url, 
                params=self.params, 
                headers=self.headers, 
                timeout=20
            )
            
            # Verificação de Rate Limit (NASA usa isso, conforme o texto que você enviou)
            remaining = response.headers.get('X-RateLimit-Remaining')
            if remaining and int(remaining) < 5:
                logger.warning(f"Rate Limit crítico para {self.endpoint_name}: {remaining} restantes.")

            response.raise_for_status()
            data = response.json()

            # Lógica robusta para json_path
            if self.json_path:
                for key in self.json_path.split('.'):
                    if isinstance(data, dict):
                        data = data.get(key)
                    else:
                        logger.error(f"Erro de estrutura no JSON: Chave '{key}' não encontrada.")
                        return pd.DataFrame() # Retorna vazio em vez de crashar o loop

            df = pd.json_normalize(data)

            if df.empty:
                logger.warning(f"Nenhum dado encontrado no endpoint {self.endpoint_name}.")
            else:
                logger.info(f"Extração concluída: {len(df)} registros para {self.endpoint_name}.")
            
            return df
            
        except requests.exceptions.SSLError:
            logger.error(f"Erro de SSL em {self.endpoint_name}. Verifique certificados ou proxy.")
            raise
        except requests.exceptions.HTTPError as e:
            # Se for 403 (NASA), o log precisa ser específico
            if e.response.status_code == 403:
                logger.error(f"Acesso Negado (403) em {self.endpoint_name}. Verifique a API Key.")
            raise
        except Exception as e:
            logger.critical(f"Falha catastrófica em {self.endpoint_name}: {e}")
            raise