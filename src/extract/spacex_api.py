import requests
import structlog
from typing import List, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pydantic import ValidationError

from src.extract.schemas import ENDPOINT_SCHEMAS
from src.config.settings import settings

logger = structlog.get_logger()

class SpaceXExtractor:
    def __init__(self):
        self.base_url = settings.SPACEX_API_URL
        self.session = self._build_session() # Corrigido: chamada de método
    
    def _build_session(self) -> requests.Session: # Corrigido: typo biuld -> build
        session = requests.Session()
        retry_strategy = Retry(
            total=settings.RETRIES,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    
    def fetch_data(self, endpoint: str) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/{endpoint}"
        log = logger.bind(endpoint=endpoint, url=url)

        try:
            log.info('Initialize request')
            response = self.session.get(url, timeout=settings.TIMEOUT)
            response.raise_for_status()
            data = response.json()

            log.info('Request successful', count=len(data))

            schema = ENDPOINT_SCHEMAS.get(endpoint)
            if schema:
                try:
                    # Validamos e transformamos para garantir o contrato
                    validated_data = [schema(**item).model_dump() for item in data]
                    log.info('Data validation successful')
                    return validated_data
                except ValidationError as ve:
                    # RIGOR: Se o contrato falhou, o pipeline deve parar ou alertar criticamente
                    log.error("Data validation failed", error=ve.errors())
                    raise # Não retorne dados sujos se a validação falhar

            return data

        except requests.exceptions.HTTPError as e:
            log.error("Request failed", error=str(e), status_code=response.status_code)
            raise 
        except requests.exceptions.Timeout as e:
            log.error("Request timed out", error=str(e))
            raise
        except Exception as e:
            log.error("An unexpected error occurred", error=str(e))
            raise

# Singleton instanciado corretamente
spacex_client = SpaceXExtractor()