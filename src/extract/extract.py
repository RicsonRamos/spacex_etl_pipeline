import requests
import json
import time
from datetime import datetime
from src.config.config import settings
from src.utils.logger import setup_logger

class SpaceXExtractor:
    """
    Extracts data from the SpaceX API.
    """
    def __init__(self):
        """
        Initializes the extractor.
        """
        self.base_url = settings.SPACEX_API_URL
        self.endpoints = settings.API_ENDPOINTS
        self.timeout = settings.TIMEOUT
        self.retries = settings.RETRIES
        self.raw_data_dir = settings.RAW_DATA_DIR
        self.logger = setup_logger("extractor")
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.session.headers.update({"User-Agent": "SpaceX-ETL/0.1"})
        
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)

    def _request(self, url):
        """
        Executes the request with strict exponential backoff.
        :param url: The URL to request.
        :return: The JSON response.
        :raises Exception: If the request fails after all retries.
        """
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                wait_time = 2 ** attempt
                self.logger.warning(f"Attempt {attempt}/{self.retries} failed: {url}. Error: {e}. Retrying in {wait_time}s...")
                if attempt == self.retries:
                    self.logger.error(f"Retry limit exhausted for: {url}")
                    raise
                time.sleep(wait_time)

    def fetch_all(self):
        """
        Extracts all endpoints defined in the manifest.
        :return: dict { 'endpoint_name': [raw_data] }
        """
        all_data = {}
        for name, config in self.endpoints.items():
            path = config.get("path")
            if not path:
                self.logger.error(f"Endpoint '{name}' ignored: 'path' not defined in manifest.")
                continue
            
            url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
            self.logger.info(f"Starting extraction: {name.upper()} from {url}")

            try:
                data = self._request(url)
                if data:
                    self._save_raw(name, data)
                    all_data[name] = data
            except Exception as e:
                self.logger.error(f"Failed to extract {name}: {e}")
                continue
            
        return all_data

    def _save_raw(self, name, data):
        """
        Atomic persistence of raw data (Raw Layer).
        :param name: The name of the endpoint.
        :param data: The JSON data.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = self.raw_data_dir / f"{name}_{timestamp}.json"
        temp_file = file_path.with_suffix(".tmp")

        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            temp_file.replace(file_path)
            self.logger.info(f"Raw JSON saved: {file_path.name}")
        except OSError as e:
            self.logger.error(f"I/O error while saving {name}: {e}")
            if temp_file.exists():
                temp_file.unlink()
