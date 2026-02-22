import requests
import structlog
from typing import List, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from src.extract.schemas import API_SCHEMAS
from src.config.settings import settings

logger = structlog.get_logger()

class SpaceXExtractor:
    """Classe responsável pela comunicação bruta e validação de contrato com a API."""

    def __init__(self):
        self.session = requests.Session()
        self.timeout = settings.API_TIMEOUT
        
        # Configuração de Retentativas (Exponential Backoff)
        retries = Retry(
            total=settings.API_RETRIES,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def fetch(self, endpoint: str) -> List[Dict[str, Any]]:
        """
        Busca dados da API e aplica validação via Pydantic Schemas.
        Qualquer erro de conexão interrompe o pipeline (Fail-Fast).
        """
        url = f"{settings.SPACEX_API_URL}/{endpoint}"
        schema = API_SCHEMAS.get(endpoint)

        logger.info("Iniciando extração de API", endpoint=endpoint, url=url)

        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            logger.error("Erro crítico na requisição HTTP", endpoint=endpoint, error=str(e))
            raise

        if not schema:
            logger.warning("Schema não mapeado para este endpoint", endpoint=endpoint)
            return data

        # Validação e Parsing
        validated_data = []
        errors_count = 0

        for item in data:
            try:
                # Validação Pydantic (extra="ignore" já garante limpeza)
                obj = schema(**item)
                validated_data.append(obj.model_dump())
            except Exception as e:
                errors_count += 1
                # Registramos o erro mas mantemos o pipeline rodando para os dados válidos
                logger.debug("Falha na validação de registro individual", 
                             endpoint=endpoint, record_id=item.get("id"), error=str(e))

        if errors_count > 0:
            logger.warning("Extração finalizada com registros corrompidos", 
                           endpoint=endpoint, valid=len(validated_data), skipped=errors_count)
        else:
            logger.info("Extração concluída com sucesso", 
                        endpoint=endpoint, count=len(validated_data))

        return validated_data
