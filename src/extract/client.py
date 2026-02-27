from typing import Any, Dict, List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from src.config.settings import settings


class SpaceXAPIClient:
    """HTTP client responsible only for communicating with SpaceX API."""

    def __init__(self):
        self.session = requests.Session()
        retries = Retry(
            total=settings.API_RETRIES,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def get(self, endpoint: str) -> List[Dict[str, Any]]:
        url = f"{settings.SPACEX_API_URL}/{endpoint}"
        response = self.session.get(url, timeout=settings.API_TIMEOUT)
        response.raise_for_status()
        return response.json()