from typing import Any, Dict, List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

class SpaceXAPIClient:
    """HTTP client for SpaceX API with robust retry logic."""

    def __init__(self, base_url: str, retries: int = 3, timeout: int = 10):
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
        retry_strategy = Retry(
            total=retries,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

    def get(self, endpoint: str) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/{endpoint}"
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        return response.json()
