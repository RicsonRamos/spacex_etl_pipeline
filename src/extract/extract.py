import requests
import json
import time
from datetime import datetime
from pathlib import Path
from src.config.config import settings
from src.utils.logger import setup_logger

class SpaceXExtractor:
    def __init__(self):
        # AUTONOMIA: Centraliza as configs do objeto settings
        self.base_url = settings.SPACEX_API_URL
        self.endpoints = settings.API_ENDPOINTS # Agora contém {'nome': {'path': '...', 'mapping': {...}}}
        self.timeout = settings.TIMEOUT
        self.retries = settings.RETRIES
        self.raw_data_dir = settings.RAW_DATA_DIR
        self.logger = setup_logger("extractor")
        
        # Garante o diretório de destino
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)

    def _request(self, url):
        """Executa a requisição com Backoff Exponencial rigoroso."""
        for attempt in range(1, self.retries + 1):
            try:
                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                wait_time = 2 ** attempt
                self.logger.warning(f"Tentativa {attempt}/{self.retries} falhou: {url}. Erro: {e}. Retentando em {wait_time}s...")
                if attempt == self.retries:
                    self.logger.error(f"Esgotadas as tentativas para: {url}")
                    raise
                time.sleep(wait_time)

    def fetch_all(self):
        """
        Extrai todos os endpoints definidos no manifesto.
        Retorna: dict { 'endpoint_name': [dados_brutos] }
        """
        all_data = {}
        
        for name, config in self.endpoints.items():
            # No novo manifesto, o path está dentro do dicionário do endpoint
            path = config.get("path")
            if not path:
                self.logger.error(f"Endpoint '{name}' ignorado: 'path' não definido no manifesto.")
                continue

            url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
            self.logger.info(f"Iniciando extração: {name.upper()} de {url}")

            try:
                data = self._request(url)
                if data:
                    self._save_raw(name, data)
                    all_data[name] = data
            except Exception as e:
                self.logger.error(f"Falha ao extrair {name}: {e}")
                continue
                
        return all_data

    def _save_raw(self, name, data):
        """Persistência atômica dos dados brutos (Raw Layer)."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = self.raw_data_dir / f"{name}_{timestamp}.json"
        temp_file = file_path.with_suffix(".tmp")

        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            temp_file.replace(file_path)
            self.logger.info(f"💾 Raw JSON salvo: {file_path.name}")
        except OSError as e:
            self.logger.error(f"Erro de I/O ao salvar {name}: {e}")
            if temp_file.exists():
                temp_file.unlink()