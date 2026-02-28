# src/extract/rockets.py
from typing import List, Dict, Any
import structlog
from .base import BaseExtractor

logger = structlog.get_logger()

# Mock data embutido no módulo (não importa de tests!)
DEFAULT_MOCK_ROCKETS = [
    {
        "id": "falcon9",
        "name": "Falcon 9",
        "active": True,
        "cost_per_launch": 50000000,
        "success_rate_pct": 98.5
    },
    {
        "id": "falconheavy",
        "name": "Falcon Heavy",
        "active": True,
        "cost_per_launch": 90000000,
        "success_rate_pct": 100.0
    }
]


class RocketExtract(BaseExtractor):
    """
    Extracts data from the SpaceX API regarding rockets.
    """

    endpoint = "rockets"

    def extract(self, real_api: bool = False) -> List[Dict[str, Any]]:
        """
        Extracts data about rockets from the SpaceX API.

        Args:
            real_api (bool): Whether to fetch data from the actual API or return mock data.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing data about rockets.
        """
        if real_api:
            logger.info("Fetching rockets from real API")
            return self.fetch(self.endpoint)
        else:
            logger.info("Returning mock rockets data")
            return DEFAULT_MOCK_ROCKETS  # ✅ Não importa de tests!