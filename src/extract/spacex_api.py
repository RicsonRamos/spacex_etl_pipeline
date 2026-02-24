import requests
import structlog
from typing import List, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from src.extract.schemas import API_SCHEMAS
from src.config.settings import get_settings

logger = structlog.get_logger()


class SpaceXExtractor:
    """
    Responsável pela comunicação com a API SpaceX
    e validação de contrato via Pydantic.
    """

    def __init__(self, settings=None, session=None):
        # Lazy loading (não executa no import)
        self.settings = settings or get_settings()
        self.session = session or requests.Session()
        self.timeout = self.settings.API_TIMEOUT

        # Retry com exponential backoff
        retries = Retry(
            total=self.settings.API_RETRIES,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
        )

        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    
    # MÉTODO GENÉRICO
    
    def fetch(self, endpoint: str) -> List[Dict[str, Any]]:
        url = f"{self.settings.SPACEX_API_URL}/{endpoint}"
        schema = API_SCHEMAS.get(endpoint)

        logger.info("Iniciando extração", endpoint=endpoint, url=url)

        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            logger.error(
                "Erro crítico na requisição HTTP",
                endpoint=endpoint,
                error=str(e),
            )
            raise

        if not schema:
            logger.warning("Schema não mapeado", endpoint=endpoint)
            return data

        validated_data = []
        errors_count = 0

        for item in data:
            try:
                obj = schema(**item)
                validated_data.append(obj.model_dump())
            except Exception as e:
                errors_count += 1
                logger.debug(
                    "Registro inválido descartado",
                    endpoint=endpoint,
                    record_id=item.get("id"),
                    error=str(e),
                )

        if errors_count:
            logger.warning(
                "Extração concluída com registros inválidos",
                endpoint=endpoint,
                valid=len(validated_data),
                skipped=errors_count,
            )
        else:
            logger.info(
                "Extração concluída com sucesso",
                endpoint=endpoint,
                count=len(validated_data),
            )

        return validated_data

    
    # COMPATIBILIDADE COM TESTES
    
    def fetch_launches(self) -> List[Dict[str, Any]]:
        return self.fetch("launches")

    def fetch_rockets(self) -> List[Dict[str, Any]]:
        return self.fetch("rockets")