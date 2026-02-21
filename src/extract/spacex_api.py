import requests
import structlog
from typing import List, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.extract.schemas import ENDPOINT_SCHEMAS
from src.config.settings import settings

logger = structlog.get_logger()

class SpaceXExtractor:
    def __init__(self):
        # Otimização: Session deve ser persistente para reuso de conexões TCP/Keep-alive
        self.session = self._setup_session()

    def _setup_session(self) -> requests.Session:
        session = requests.Session()
        retries = Retry(
            total=settings.API_RETRIES,
            backoff_factor=1,
            # 429 é vital para APIs públicas (Rate Limiting)
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False # Permite que o raise_for_status trate o erro
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session

    def fetch(self, endpoint: str) -> List[Dict[str, Any]]:
        url = f"{settings.SPACEX_API_URL}/{endpoint}"
        try:
            logger.info("Iniciando extração", endpoint=endpoint)
            response = self.session.get(url, timeout=settings.API_TIMEOUT)
            response.raise_for_status()
            
            data = response.json()
            
            # Validação Rigorosa de Schema
            schema = ENDPOINT_SCHEMAS.get(endpoint)
            if not schema:
                # Falha Crítica: Não operamos sem schema definido em produção
                logger.warning("Schema não encontrado para o endpoint. Retornando dados brutos.", endpoint=endpoint)
                return data

            # Processamento com validação Pydantic
            # Retornamos model_dump() para garantir que o Polars receba dicionários puros
            validated_data = [schema(**item).model_dump() for item in data]
            
            logger.info("Extração concluída com sucesso", endpoint=endpoint, count=len(validated_data))
            return validated_data

        except requests.exceptions.HTTPError as e:
            logger.error("Erro HTTP na API SpaceX", endpoint=endpoint, status=e.response.status_code)
            raise
        except requests.exceptions.Timeout:
            logger.error("Timeout na API SpaceX", endpoint=endpoint, timeout=settings.API_TIMEOUT)
            raise
        except Exception as e:
            logger.error("Falha inesperada na extração", endpoint=endpoint, error=str(e))
            raise