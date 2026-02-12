import requests 
import json
import time
from pathlib import Path
from datetime import datetime
from requests.exceptions import RequestException, HTTPError

class SpaceXExtractor:
    def __init__(self, config):
        # Fail-fast: se a config não tiver as chaves, o erro aparece no init
        self.base_url = config["api"]["base_url"]
        self.endpoints = config["api"]["endpoints"]
        self.timeout = config["pipeline"].get("timeout", 10)
        self.retries = config["pipeline"].get("retries", 3)
        self.raw_data_dir = Path(config["paths"]["raw"])
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        # Use o logger centralizado, mas sem criar arquivos locais dentro da src
        from src.logger import setup_logger
        self.logger = setup_logger("extractor")

    def _request(self, url):
        """Executa requisição com Backoff Exponencial."""
        for attempt in range(1, self.retries + 1):
            try:
                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            
            except (RequestException, HTTPError) as e:
                wait_time = 2 ** attempt  # 2, 4, 8 segundos
                self.logger.warning(
                    f"Tentativa {attempt}/{self.retries} falhou para {url}. "
                    f"Erro: {e}. Tentando novamente em {wait_time}s..."
                )
                if attempt == self.retries:
                    self.logger.error(f"Esgotadas as tentativas para a URL: {url}")
                    raise  # Re-lança a exceção para o main tratar
                time.sleep(wait_time)

    def fetch_endpoint(self, name, endpoint):
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        self.logger.info(f"Iniciando extração: {url}")

        data = self._request(url)
        
        if not data:
            raise ValueError(f"Nenhum dado retornado do endpoint {name}")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = self.raw_data_dir / f"{name}_{timestamp}.json"

        # Escrita atômica: evita arquivos corrompidos se o processo morrer no meio
        try:
            temp_file = file_path.with_suffix(".tmp")
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            temp_file.replace(file_path)
            self.logger.info(f"Dados salvos com sucesso: {file_path}")
        except OSError as e:
            self.logger.error(f"Falha crítica de I/O ao salvar {name}: {e}")
            raise

        return data
    
    def fetch_all(self):
        raw_data = {}
        for name, endpoint in self.endpoints.items():
            raw_data[name] = self.fetch_endpoint(name, endpoint)
        return raw_data