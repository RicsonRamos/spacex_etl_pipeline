# tests/unit/extract/test_client.py
"""
Testes para SpaceXAPIClient.
Verifica comunicação HTTP com retry e tratamento de erros.
"""

import pytest
from unittest import mock
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from src.extract.client import SpaceXAPIClient
from src.config.settings import Settings


class TestSpaceXAPIClient:
    """Testes unitários para cliente SpaceX API."""
    
    @pytest.fixture
    def mock_settings(self):
        """Settings mockadas."""
        with mock.patch('src.extract.client.settings') as mock_settings:
            mock_settings.SPACEX_API_URL = "https://api.spacexdata.com/v4"
            mock_settings.API_RETRIES = 3
            mock_settings.API_TIMEOUT = 30
            yield mock_settings
    
    @pytest.fixture
    def client(self, mock_settings):
        """Cliente com settings mockadas."""
        return SpaceXAPIClient()
    
    def test_init_creates_session(self, client):
        """Deve criar sessão HTTP."""
        assert client.session is not None
        assert isinstance(client.session, requests.Session)
    
    def test_init_configures_retry(self, client):
        """Deve configurar retry na sessão."""
        adapter = client.session.get_adapter("https://")
        assert isinstance(adapter, HTTPAdapter)
        assert adapter.max_retries.total == 3
    
    def test_get_success(self, client, mock_settings):
        """Deve retornar dados JSON em caso de sucesso."""
        mock_response = mock.Mock()
        mock_response.json.return_value = [{"id": "rocket1", "name": "Falcon 9"}]
        mock_response.raise_for_status.return_value = None
        
        with mock.patch.object(client.session, 'get', return_value=mock_response) as mock_get:
            result = client.get("rockets")
            
            mock_get.assert_called_once_with(
                "https://api.spacexdata.com/v4/rockets",
                timeout=30
            )
            assert result == [{"id": "rocket1", "name": "Falcon 9"}]
    
    def test_get_http_error(self, client, mock_settings):
        """Deve propagar erro HTTP."""
        mock_response = mock.Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        
        with mock.patch.object(client.session, 'get', return_value=mock_response):
            with pytest.raises(requests.HTTPError, match="404 Not Found"):
                client.get("invalid_endpoint")
    
    def test_get_connection_error(self, client, mock_settings):
        """Deve propagar erro de conexão."""
        with mock.patch.object(
            client.session, 'get', 
            side_effect=requests.ConnectionError("Network unreachable")
        ):
            with pytest.raises(requests.ConnectionError, match="Network unreachable"):
                client.get("rockets")
    
    def test_get_timeout(self, client, mock_settings):
        """Deve propagar timeout."""
        with mock.patch.object(
            client.session, 'get',
            side_effect=requests.Timeout("Request timed out")
        ):
            with pytest.raises(requests.Timeout, match="Request timed out"):
                client.get("rockets")
    
    def test_get_uses_correct_url_format(self, client, mock_settings):
        """Deve montar URL corretamente."""
        mock_response = mock.Mock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None
        
        with mock.patch.object(client.session, 'get', return_value=mock_response) as mock_get:
            client.get("launches/latest")
            
            mock_get.assert_called_once_with(
                "https://api.spacexdata.com/v4/launches/latest",
                timeout=30
            )
    
    def test_retry_configuration(self, client):
        """Deve ter retry configurado para erros 5xx."""
        adapter = client.session.get_adapter("https://")
        retry = adapter.max_retries
        
        assert retry.total == 3
        assert retry.backoff_factor == 1
        assert 500 in retry.status_forcelist
        assert 502 in retry.status_forcelist
        assert 503 in retry.status_forcelist
        assert 504 in retry.status_forcelist
        assert 404 not in retry.status_forcelist  # Não deve retry em 4xx