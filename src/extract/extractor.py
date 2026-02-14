import requests
import time
from src.config.config import settings
from src.utils.logger import setup_logger

class SpaceXExtractor:
    def __init__(self):
        self.base_url = settings.SPACEX_API_URL.rstrip("/")
        self.timeout = settings.TIMEOUT
        self.retries = settings.RETRIES
        self.logger = setup_logger("extractor")
        
        # Uso de Session para reutilizar conexões TCP (Escalabilidade)
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "SpaceX-Prod-Pipeline/1.0"
        })

    def extract(self, path: str) -> list:
        """Extrai dados de um path específico com lógica de retry."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout)
                
                # RIGOR: Se for 404, o endpoint mudou ou não existe. Não retente.
                if response.status_code == 404:
                    self.logger.error(f"Endpoint não encontrado (404): {url}")
                    return []
                
                response.raise_for_status()
                data = response.json()
                
                # Garante que sempre retornamos uma lista para o Transformer
                return data if isinstance(data, list) else [data]
                
            except requests.exceptions.RequestException as e:
                wait = 2 ** attempt # Backoff exponencial
                self.logger.warning(f"Falha na tentativa {attempt}/{self.retries} para {url}: {e}")
                if attempt == self.retries:
                    self.logger.error(f"Limite de tentativas esgotado para: {url}")
                    raise e
                time.sleep(wait)
        return []