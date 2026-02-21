import requests
import structlog
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from src.extract.schemas import API_SCHEMAS
from src.config.settings import settings

logger = structlog.get_logger()

class SpaceXExtractor:
    def __init__(self):
        self.session = requests.Session()
        # Configuração de timeout global para evitar que o worker do Prefect trave
        self.timeout = getattr(settings, "API_TIMEOUT", 30)

    def fetch(
        self, 
        endpoint: str, 
        incremental: bool = False, 
        last_ingested: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Busca dados da API e aplica validação Pydantic.
        Suporta lógica incremental filtrando por data de referência.
        """
        url = f"{settings.SPACEX_API_URL}/{endpoint}"
        schema = API_SCHEMAS.get(endpoint)

        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            logger.error("Falha na requisição HTTP", endpoint=endpoint, error=str(e))
            raise

        # 1. FILTRAGEM INCREMENTAL (Client-side)
        # Se for launches e houver data de referência, filtramos antes da validação pesada
        if incremental and last_ingested and endpoint == "launches":
            data = self._filter_incremental(data, last_ingested)
            logger.info("Filtro incremental aplicado", endpoint=endpoint, remaining=len(data))

        if not schema:
            logger.warning("Nenhum schema Pydantic encontrado para o endpoint", endpoint=endpoint)
            return data

        # 2. VALIDAÇÃO E PARSE
        validated = []
        for item in data:
            try:
                # Validação via Pydantic definida no Script 2 original
                obj = schema(**item)
                validated.append(obj.model_dump())
            except Exception as e:
                # Logamos o erro mas não paramos o pipeline por causa de um registro sujo
                logger.error(
                    "Payload inválido detectado", 
                    endpoint=endpoint, 
                    error=str(e), 
                    record_id=item.get("id")
                )
        
        return validated

    def _filter_incremental(self, data: List[Dict], last_date: datetime) -> List[Dict]:
        """Filtra a lista de dicionários baseando-se no campo date_utc."""
        filtered_data = []
        for item in data:
            date_str = item.get("date_utc")
            if not date_str:
                continue
            
            try:
                # Converte ISO string da API para datetime aware para comparação segura
                # A API SpaceX usa formato UTC (Z)
                record_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                
                if record_date > last_date:
                    filtered_data.append(item)
            except ValueError:
                continue
                
        return filtered_data
