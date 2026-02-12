import requests 
import json
from pathlib import Path
from datetime import datetime

from src.logger import setup_logger

class SpaceXExtractor:
    def __init__(self, config):
        self.base_url = config["api"]["base_url"]
        self.endpoints = config["api"]["endpoints"]

        self.timeout = config["pipeline"]["timeout"]
        self.retries = config["pipeline"]["retries"]

        self.raw_data_dir = Path(config["paths"]["raw"])
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)

        self.logger = setup_logger("extractor", "extractor.log")

    def _request(self, url):
        for attempt in range(self.retries):
            try:
                response = requests.get(
                    url,
                    timeout=self.timeout 
                )
                response.raise_for_status()

                return response.json()
            
            except Exception as e:
                
                self.logger.warning(
                    f"Attempt {attempt}/{self.retries} failed: {e}"
                )

                if attempt == self.retries:
                    raise
    def fetch_endpoint(self, name, endpoint):

        url = f"{self.base_url}{endpoint}"

        self.logger.info(f"Fetching data from {url}")

        data = self._request(url)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        file_path = self.raw_data_dir / f"{name}_{timestamp}.json"

        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        self.logger.info(f"Data saved to {file_path}")

        return data
    
    def fetch_all(self):

        raw_data = {}

        for name, endpoint in self.endpoints.items():
            raw_data[name] = self.fetch_endpoint(name, endpoint)

        return raw_data