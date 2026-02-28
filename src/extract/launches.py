# src/extract/launches.py
from typing import List, Dict, Any
from src.extract.base import BaseExtractor
import structlog

logger = structlog.get_logger()

# Mock data embutido no módulo (não importa de tests!)
DEFAULT_MOCK_LAUNCHES = [
    {
        "id": "launch1",
        "name": "Starlink 1",
        "date_utc": "2024-01-01T00:00:00Z",
        "success": True,
        "rocket": "falcon9"
    },
    {
        "id": "launch2",
        "name": "Starlink 2",
        "date_utc": "2024-01-15T00:00:00Z",
        "success": True,
        "rocket": "falcon9"
    }
]


class LaunchExtract(BaseExtractor):
    """
    Extracts data from the SpaceX API, specifically the launches endpoint.
    """

    endpoint = "launches"

    def extract(self, real_api: bool = False) -> List[Dict[str, Any]]:
        """
        Returns a list of dictionaries containing the launches data.

        Args:
            real_api (bool): Whether to fetch real data from the API or return a mock dataset.

        Returns:
            List[Dict[str, Any]]: Launches data as a list of dictionaries.
        """
        if real_api:
            logger.info("Fetching launches from real API")
            return self.fetch(self.endpoint)
        else:
            logger.info("Returning mock launches data")
            return DEFAULT_MOCK_LAUNCHES  # ✅ Não importa de tests!