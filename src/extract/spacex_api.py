import requests
import structlog
from typing import List, Dict, Any, Optional
from src.extract.schemas import API_SCHEMAS
from src.config.settings import settings

logger = structlog.get_logger()

class SpaceXExtractor:
    def __init__(self):
        self.session = requests.Session()

    def fetch(self, endpoint: str) -> List[Dict[str, Any]]:
        url = f"{settings.SPACEX_API_URL}/{endpoint}"
        schema = API_SCHEMAS.get(endpoint)
        
        response = self.session.get(url, timeout=settings.API_TIMEOUT)
        response.raise_for_status()
        data = response.json()

        if not schema:
            return data

        validated = []
        for item in data:
            try:
                # Validação estrita: se falhar, logamos o ID para investigação
                obj = schema(**item)
                validated.append(obj.model_dump())
            except Exception as e:
                logger.error("Payload inválido", endpoint=endpoint, error=str(e), id=item.get("id"))
        
        return validated
