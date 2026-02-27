from typing import List, Dict, Any
from src.extract.base import BaseExtractor
import structlog

logger = structlog.get_logger()

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
            from tests.unit.extract.conftest import MOCK_LAUNCHES
            return MOCK_LAUNCHES