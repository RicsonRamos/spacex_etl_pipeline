from typing import List, Dict, Any
import structlog
from .base import BaseExtractor

logger = structlog.get_logger()

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
            from tests.unit.extract.conftest import MOCK_ROCKETS
            return MOCK_ROCKETS