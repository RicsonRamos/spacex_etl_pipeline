from typing import Any, Dict, List, Type, Optional
import requests
import structlog
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from pydantic import BaseModel, ValidationError
from src.config.settings import settings
from src.extract.schemas import API_SCHEMAS

logger = structlog.get_logger()

class BaseExtractor:
    """Base class for API communication and Pydantic validation.

    This class handles the communication with the SpaceX API and
    validates the received data using Pydantic models.
    """

    def __init__(self):
        """Initialize the extractor.

        Setup the requests session with a custom timeout and
        a retry policy.
        """
        self.session = requests.Session()
        self.timeout = settings.API_TIMEOUT
        retries = Retry(total=settings.API_RETRIES, backoff_factor=1, status_forcelist=[500,502,503,504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def request(self, endpoint: str) -> List[Dict[str, Any]]:
        """Perform an HTTP GET request to the SpaceX API.

        Args:
            endpoint: The endpoint to query.

        Returns:
            The JSON response from the API.
        """
        url = f"{settings.SPACEX_API_URL}/{endpoint}"
        logger.info("Starting API request", endpoint=endpoint, url=url)
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error("HTTP request failed", endpoint=endpoint, error=str(e))
            raise

    def validate(self, data: List[Dict[str, Any]], schema: Optional[Type[BaseModel]]) -> List[Dict[str, Any]]:
        """Validate a list of dictionaries using a Pydantic model.

        Args:
            data: The list of dictionaries to validate.
            schema: The Pydantic model to use for validation.

        Returns:
            The validated list of dictionaries.
        """
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
            logger.warning("Validation finished with skipped records", valid=len(validated), skipped=errors)
        else:
            logger.info("Validation completed successfully", count=len(validated))
        return validated

    def fetch(self, endpoint: str) -> List[Dict[str, Any]]:
        """Fetch data from the SpaceX API and validate it.

        Args:
            endpoint: The endpoint to query.

        Returns:
            The validated list of dictionaries.
        """
        raw = self.request(endpoint)
        schema = API_SCHEMAS.get(endpoint)
        return self.validate(raw, schema)
