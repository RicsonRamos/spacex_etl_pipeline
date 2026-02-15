import requests
from typing import Any, Dict, List
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from requests.exceptions import RequestException
from src.config.config import settings
from src.utils.logger import setup_logger


class SpaceXExtractor:
    """
    SpaceX API data extractor.

    This class is responsible for retrieving data from the configured SpaceX API
    endpoint with retry logic, structured logging, and robust error handling.

    Features:
        - Persistent HTTP session for connection reuse
        - Exponential backoff retry strategy
        - Automatic JSON parsing
        - Structured logging
        - Configurable timeout and retry attempts

    Raises:
        RequestException: If the request fails after retry attempts.
        ValueError: If the API response cannot be parsed as JSON.
    """

    def __init__(self) -> None:
        """Initialize extractor with configuration and HTTP session."""
        self.base_url: str = settings.SPACEX_API_URL.rstrip("/")
        self.timeout: int = settings.TIMEOUT
        self.retries: int = settings.RETRIES
        self.logger = setup_logger("spacex_extractor")

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "SpaceX-Prod-Pipeline/2.0",
            }
        )

    @retry(
        stop=stop_after_attempt(settings.RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(RequestException),
        reraise=True,
    )
    def _make_request(self, url: str) -> requests.Response:
        """
        Perform an HTTP GET request with retry support.

        The retry policy uses exponential backoff and retries only for
        RequestException errors.

        Args:
            url (str): Fully qualified URL.

        Returns:
            requests.Response: HTTP response object.

        Raises:
            RequestException: If request repeatedly fails.
        """
        self.logger.debug(f"Requesting URL: {url}")

        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()

        return response

    def extract(self, path: str) -> List[Dict[str, Any]]:
        """
        Extract JSON data from a given API path.

        This method builds the full API URL, performs a retried request,
        and parses the JSON response.

        Args:
            path (str): API endpoint path (e.g., '/launches').

        Returns:
            List[Dict[str, Any]]: Parsed JSON records.
            If the API returns a single object, it is wrapped into a list.

        Raises:
            RequestException: If HTTP request fails.
            ValueError: If JSON parsing fails.
        """
        if not path or not isinstance(path, str):
            raise ValueError("Path must be a non-empty string.")

        url = f"{self.base_url}/{path.lstrip('/')}"
        self.logger.info(f"Extracting data from: {url}")

        response = self._make_request(url)

        try:
            data = response.json()
        except ValueError as exc:
            self.logger.exception("Failed to decode JSON response.")
            raise ValueError("Invalid JSON response from API.") from exc

        if isinstance(data, list):
            return data

        return [data]

    def close(self) -> None:
        """
        Close the underlying HTTP session.

        Should be called when the extractor is no longer needed
        to properly release network resources.
        """
        self.session.close()
        self.logger.debug("HTTP session closed.")
