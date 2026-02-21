import requests
import structlog
from typing import List, Dict, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.extract.schemas import ENDPOINT_SCHEMAS
from src.config.settings import settings
from datetime import datetime

logger = structlog.get_logger()


class SpaceXExtractor:
    def __init__(self):
        # Reuso de sessão TCP/Keep-alive
        self.session = self._setup_session()

    def _setup_session(self) -> requests.Session:
        session = requests.Session()
        retries = Retry(
            total=settings.API_RETRIES,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session

    def fetch(
        self, 
        endpoint: str, 
        incremental: bool = False, 
        last_ingested: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Busca dados do endpoint SpaceX.
        Se incremental=True, filtra por last_ingested.
        """

        url = f"{settings.SPACEX_API_URL}/{endpoint}"

        try:
            logger.info("Iniciando extração", endpoint=endpoint, incremental=incremental)
            response = self.session.get(url, timeout=settings.API_TIMEOUT)
            response.raise_for_status()
            data = response.json()

            # Filtragem incremental
            if incremental and last_ingested:
                # Considera apenas itens com 'date_utc' maior que last_ingested
                filtered_data = [
                    item for item in data 
                    if "date_utc" in item and datetime.fromisoformat(item["date_utc"].replace("Z", "+00:00")) > last_ingested
                ]
                logger.info("Filtragem incremental aplicada", endpoint=endpoint, original=len(data), filtered=len(filtered_data))
                data = filtered_data

            # Validação de schema
            schema = ENDPOINT_SCHEMAS.get(endpoint)
            if schema:
                validated_data = [schema(**item).model_dump() for item in data]
            else:
                logger.warning("Schema não encontrado, retornando dados brutos", endpoint=endpoint)
                validated_data = data

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