from typing import Any, Dict, List, Type, Optional
from pydantic import BaseModel, ValidationError
import structlog

from src.extract.client import SpaceXAPIClient
from src.extract.schemas import API_SCHEMAS

logger = structlog.get_logger()


class BaseExtractor:
    """Handles validation and extraction flow."""

    def __init__(self, client: Optional[SpaceXAPIClient] = None):
        self.client = client or SpaceXAPIClient()

    def validate(
        self,
        data: List[Dict[str, Any]],
        schema: Optional[Type[BaseModel]],
    ) -> List[Dict[str, Any]]:
        if not schema:
            logger.warning("No schema provided, returning raw data")
            return data

        validated = []
        errors = 0

        for item in data:
            try:
                validated.append(schema(**item).model_dump())
            except ValidationError as e:
                errors += 1
                logger.debug("Validation failed", record_id=item.get("id"), error=str(e))

        if errors > 0:
            logger.warning(
                "Validation finished with skipped records",
                valid=len(validated),
                skipped=errors,
            )
        else:
            logger.info("Validation completed successfully", count=len(validated))

        return validated

    def fetch(self, endpoint: str) -> List[Dict[str, Any]]:
        raw = self.client.get(endpoint)
        schema = API_SCHEMAS.get(endpoint)
        return self.validate(raw, schema)