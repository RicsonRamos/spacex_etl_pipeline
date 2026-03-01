from typing import Any, Dict, List, Type, Optional
from pydantic import BaseModel, ValidationError
import structlog

logger = structlog.get_logger()

class BaseExtractor:
    """Orchestrates extraction and validation via Pydantic schemas."""

    def __init__(self, client: Any, schema: Optional[Type[BaseModel]] = None):
        self.client = client
        self.schema = schema

    def _validate(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not self.schema:
            return data

        try:
            # Validação em lote para garantir integridade total
            return [self.schema(**item).model_dump() for item in data]
        except ValidationError as e:
            logger.error("Schema validation failed - Data integrity at risk", error=str(e))
            raise  # Interrompe o pipeline (Fail-fast)

    def extract(self, endpoint: str) -> List[Dict[str, Any]]:
        raw_data = self.client.get(endpoint)
        return self._validate(raw_data)
