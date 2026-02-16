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
    """
    Singleton class responsible for extracting data from the SpaceX API.
    """
    def __init__(self):
        """
        Initialize the extractor with the base URL and a session.
        """
        self.base_url = settings.SPACEX_API_URL
        self.session = self._build_session()
    
    def _build_session(self) -> requests.Session:
        """
        Build a session with a custom retry strategy.
        """
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
        """
        Fetch data from the specified endpoint.
        """
        url = f"{self.base_url}/{endpoint}"
        log = logger.bind(endpoint=endpoint, url=url)

        try:
            # Initialize the request
            log.info('Initialize request')
            response = self.session.get(url, timeout=settings.TIMEOUT)
            response.raise_for_status()

            # Parse the response
            data = response.json()

            # Log the successful request
            log.info('Request successful', count=len(data))

            # Validate the data using the associated schema
            schema = ENDPOINT_SCHEMAS.get(endpoint)
            if schema:
                try:
                    # Validate each item in the data
                    validated_data = [schema(**item).model_dump() for item in data]
                    log.info('Data validation successful')
                    return validated_data
                except ValidationError as ve:
                    # Log the validation error
                    log.error("Data validation failed", error=ve.errors())
                    raise 
            # Return the data as is if no schema is associated
            return data

        except requests.exceptions.HTTPError as e:
            # Log the request failure
            log.error("Request failed", error=str(e), status_code=response.status_code)
            raise 
        except requests.exceptions.Timeout as e:
            # Log the request timeout
            log.error("Request timed out", error=str(e))
            raise 
        except Exception as e:
            # Log any unexpected error
            log.error("An unexpected error occurred", error=str(e))
            raise

# Singleton instanciado corretamente
spacex_client = SpaceXExtractor()