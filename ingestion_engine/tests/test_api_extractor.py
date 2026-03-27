"""
Testes completos para APIExtractor.
"""

import pytest
import pandas as pd
import requests
from unittest.mock import Mock, patch, MagicMock

from src.extractors.concrete_extractors import APIExtractor


# =============================================================================
# CLASSE: TestAPIExtractorInit
# =============================================================================

class TestAPIExtractorInit:
    """Testes de inicialização e configuração do APIExtractor."""
    
    def test_init_basic(self):
        """Testa inicialização com parâmetros mínimos."""
        extractor = APIExtractor(
            endpoint_name="spacex_test",
            url="https://api.spacexdata.com/v4/launches"
        )
        
        assert extractor.endpoint_name == "spacex_test"
        assert extractor.url == "https://api.spacexdata.com/v4/launches"
        assert extractor.params is None
        assert extractor.headers is None
        assert extractor.json_path is None
        assert extractor.session is not None
    
    def test_init_with_all_params(self):
        """Testa inicialização com todos os parâmetros."""
        extractor = APIExtractor(
            endpoint_name="nasa_complex",
            url="https://api.nasa.gov/DONKI/SEP",
            params={"api_key": "test_key", "startDate": "2026-01-01"},
            headers={"Accept": "application/json", "X-Custom-Header": "test"},
            json_path="data.events"
        )
        
        assert extractor.params == {"api_key": "test_key", "startDate": "2026-01-01"}
        assert extractor.headers == {"Accept": "application/json", "X-Custom-Header": "test"}
        assert extractor.json_path == "data.events"
    
    def test_session_retry_configuration(self):
        """Verifica se retry está configurado corretamente na sessão."""
        extractor = APIExtractor(endpoint_name="test", url="https://test.com")
        
        adapter = extractor.session.adapters.get('https://')
        assert adapter is not None
        assert adapter.max_retries.total == 3
        assert adapter.max_retries.backoff_factor == 1
        assert 500 in adapter.max_retries.status_forcelist
        assert 502 in adapter.max_retries.status_forcelist
        assert 503 in adapter.max_retries.status_forcelist
        assert 504 in adapter.max_retries.status_forcelist


# =============================================================================
# CLASSE: TestAPIExtractorExtractSuccess
# =============================================================================

class TestAPIExtractorExtractSuccess:
    """Testes de extração bem-sucedida."""
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_success_basic(self, mock_get, sample_spacex_launches, mock_response_success):
        """Testa extração básica bem-sucedida."""
        mock_response_success.json.return_value = sample_spacex_launches
        mock_get.return_value = mock_response_success
        
        extractor = APIExtractor(
            endpoint_name="spacex_launches",
            url="https://api.spacexdata.com/v4/launches"
        )
        
        result = extractor.extract()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert "id" in result.columns
        assert "flight_number" in result.columns
        assert "name" in result.columns
        assert "date_utc" in result.columns
        
        mock_get.assert_called_once_with(
            "https://api.spacexdata.com/v4/launches",
            params=None,
            headers=None,
            timeout=20
        )
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_success_with_params(self, mock_get, sample_spacex_launches, mock_response_success):
        """Testa extração com parâmetros de query."""
        mock_response_success.json.return_value = sample_spacex_launches
        mock_get.return_value = mock_response_success
        
        extractor = APIExtractor(
            endpoint_name="spacex_filtered",
            url="https://api.spacexdata.com/v4/launches",
            params={"limit": 10, "offset": 0}
        )
        
        result = extractor.extract()
        
        assert len(result) == 3
        mock_get.assert_called_once_with(
            "https://api.spacexdata.com/v4/launches",
            params={"limit": 10, "offset": 0},
            headers=None,
            timeout=20
        )
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_success_with_headers(self, mock_get, sample_spacex_launches, mock_response_success):
        """Testa extração com headers customizados."""
        mock_response_success.json.return_value = sample_spacex_launches
        mock_get.return_value = mock_response_success
        
        extractor = APIExtractor(
            endpoint_name="spacex_auth",
            url="https://api.spacexdata.com/v4/launches",
            headers={"Authorization": "Bearer test_token"}
        )
        
        result = extractor.extract()
        
        mock_get.assert_called_once_with(
            "https://api.spacexdata.com/v4/launches",
            params=None,
            headers={"Authorization": "Bearer test_token"},
            timeout=20
        )
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_with_json_path_single_level(self, mock_get, mock_response_success):
        """Testa extração com json_path de nível único."""
        data = {"events": [{"id": "1", "name": "Event 1"}, {"id": "2", "name": "Event 2"}]}
        mock_response_success.json.return_value = data
        mock_get.return_value = mock_response_success
        
        extractor = APIExtractor(
            endpoint_name="nasa_events",
            url="https://api.nasa.gov/events",
            json_path="events"
        )
        
        result = extractor.extract()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "id" in result.columns
        assert "name" in result.columns
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_with_json_path_nested(self, mock_get, nasa_nested_response, mock_response_success):
        """Testa extração com json_path aninhado (data.events)."""
        mock_response_success.json.return_value = nasa_nested_response
        mock_get.return_value = mock_response_success
        
        extractor = APIExtractor(
            endpoint_name="nasa_nested",
            url="https://api.nasa.gov/DONKI/SEP",
            json_path="data.events"
        )
        
        result = extractor.extract()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert "activityID" in result.columns
        assert "startTime" in result.columns
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_single_object_response(self, mock_get, mock_response_success):
        """Testa quando API retorna objeto único em vez de lista."""
        mock_response_success.json.return_value = {
            "id": "single",
            "name": "Single Object",
            "value": 100
        }
        mock_get.return_value = mock_response_success
        
        extractor = APIExtractor(
            endpoint_name="single_object",
            url="https://api.test.com/single"
        )
        
        result = extractor.extract()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["id"] == "single"
        assert result.iloc[0]["name"] == "Single Object"
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_empty_list(self, mock_get, mock_response_success):
        """Testa extração com resposta vazia (lista vazia)."""
        mock_response_success.json.return_value = []
        mock_get.return_value = mock_response_success
        
        extractor = APIExtractor(
            endpoint_name="empty_endpoint",
            url="https://api.test.com/empty"
        )
        
        result = extractor.extract()
        
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        assert len(result.columns) == 0


# =============================================================================
# CLASSE: TestAPIExtractorRateLimit
# =============================================================================

class TestAPIExtractorRateLimit:
    """Testes de comportamento com rate limiting."""
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_rate_limit_warning(self, mock_get, sample_spacex_launches, 
                                        mock_response_rate_limit_critical, caplog):
        """Testa warning quando rate limit está crítico (< 5)."""
        import logging
        
        mock_response_rate_limit_critical.json.return_value = sample_spacex_launches
        mock_get.return_value = mock_response_rate_limit_critical
        
        extractor = APIExtractor(
            endpoint_name="rate_limited_api",
            url="https://api.test.com/data"
        )
        
        with caplog.at_level(logging.WARNING):
            result = extractor.extract()
        
        assert "Rate Limit crítico" in caplog.text
        assert "3 restantes" in caplog.text
        assert len(result) == 3
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_rate_limit_normal_no_warning(self, mock_get, sample_spacex_launches, 
                                                   mock_response_rate_limit_normal, caplog):
        """Testa que não há warning quando rate limit é normal."""
        import logging
        
        mock_response_rate_limit_normal.json.return_value = sample_spacex_launches
        mock_get.return_value = mock_response_rate_limit_normal
        
        extractor = APIExtractor(
            endpoint_name="normal_api",
            url="https://api.test.com/data"
        )
        
        with caplog.at_level(logging.WARNING):
            result = extractor.extract()
        
        assert "Rate Limit crítico" not in caplog.text
        assert len(result) == 3
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_no_rate_limit_header(self, mock_get, sample_spacex_launches, caplog):
        """Testa comportamento quando header de rate limit não está presente."""
        import logging
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = sample_spacex_launches
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        extractor = APIExtractor(
            endpoint_name="no_rate_limit_header",
            url="https://api.test.com/data"
        )
        
        with caplog.at_level(logging.WARNING):
            result = extractor.extract()
        
        assert len(result) == 3


# =============================================================================
# CLASSE: TestAPIExtractorErrors
# =============================================================================

class TestAPIExtractorErrors:
    """Testes de tratamento de erros."""
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_ssl_error(self, mock_get):
        """Testa tratamento de erro SSL."""
        from requests.exceptions import SSLError
        
        mock_get.side_effect = SSLError("SSL Certificate verify failed")
        
        extractor = APIExtractor(
            endpoint_name="ssl_test",
            url="https://api.test.com"
        )
        
        with pytest.raises(SSLError):
            extractor.extract()
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_connection_error(self, mock_get):
        """Testa tratamento de erro de conexão."""
        from requests.exceptions import ConnectionError
        
        mock_get.side_effect = ConnectionError("Connection refused")
        
        extractor = APIExtractor(
            endpoint_name="connection_test",
            url="https://api.test.com"
        )
        
        with pytest.raises(ConnectionError):
            extractor.extract()
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_timeout_error(self, mock_get):
        """Testa tratamento de timeout."""
        from requests.exceptions import Timeout
        
        mock_get.side_effect = Timeout("Request timed out after 20s")
        
        extractor = APIExtractor(
            endpoint_name="timeout_test",
            url="https://slow.api.com"
        )
        
        with pytest.raises(Timeout):
            extractor.extract()
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_http_403_error(self, mock_get):
        """Testa tratamento específico de erro 403 (NASA API Key)."""
        from requests.exceptions import HTTPError
        
        # Cria mock response completo com response na exceção
        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.headers = {}
        
        # Cria HTTPError com response configurado corretamente
        http_error = HTTPError("403 Client Error: Forbidden")
        http_error.response = mock_response
        
        mock_response.raise_for_status.side_effect = http_error
        mock_get.return_value = mock_response
        
        extractor = APIExtractor(
            endpoint_name="nasa_protected",
            url="https://api.nasa.gov/protected"
        )
        
        with pytest.raises(HTTPError) as exc_info:
            extractor.extract()
        
        # Verifica que a exceção foi lançada e tem response
        assert exc_info.value.response is not None
        assert exc_info.value.response.status_code == 403
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_http_500_error(self, mock_get):
        """Testa tratamento de erro 500."""
        from requests.exceptions import HTTPError
        
        # Cria resposta 500 com HTTPError completo
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.headers = {}
        
        http_error = HTTPError("500 Server Error: Internal Server Error")
        http_error.response = mock_response
        mock_response.raise_for_status.side_effect = http_error
        
        mock_get.return_value = mock_response
        
        extractor = APIExtractor(
            endpoint_name="unstable_api",
            url="https://api.test.com/unstable"
        )
        
        with pytest.raises(HTTPError) as exc_info:
            extractor.extract()
        
        # Verifica que foi chamado pelo menos uma vez
        assert mock_get.call_count >= 1
        # Verifica que o erro é 500
        assert exc_info.value.response.status_code == 500
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_http_404_error(self, mock_get):
        """Testa tratamento de erro 404 Not Found."""
        from requests.exceptions import HTTPError
        
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.headers = {}
        
        http_error = HTTPError("404 Client Error: Not Found")
        http_error.response = mock_response
        mock_response.raise_for_status.side_effect = http_error
        
        mock_get.return_value = mock_response
        
        extractor = APIExtractor(
            endpoint_name="not_found",
            url="https://api.test.com/notfound"
        )
        
        with pytest.raises(HTTPError) as exc_info:
            extractor.extract()
        
        assert exc_info.value.response.status_code == 404


# =============================================================================
# CLASSE: TestAPIExtractorEdgeCases
# =============================================================================

class TestAPIExtractorEdgeCases:
    """Testes de casos extremos e edge cases."""
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_deeply_nested_json_path(self, mock_get, mock_response_success):
        """Testa json_path com múltiplos níveis de aninhamento."""
        deep_data = {
            "level1": {
                "level2": {
                    "level3": [
                        {"id": "1", "value": "a"},
                        {"id": "2", "value": "b"}
                    ]
                }
            }
        }
        mock_response_success.json.return_value = deep_data
        mock_get.return_value = mock_response_success
        
        extractor = APIExtractor(
            endpoint_name="deep_nested",
            url="https://api.test.com",
            json_path="level1.level2.level3"
        )
        
        result = extractor.extract()
        
        assert len(result) == 2
        assert "value" in result.columns
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_invalid_json_path_returns_empty(self, mock_get, mock_response_success, caplog):
        """Testa comportamento quando json_path é inválido."""
        import logging
        
        mock_response_success.json.return_value = {"other": "data"}
        mock_get.return_value = mock_response_success
        
        extractor = APIExtractor(
            endpoint_name="broken_path",
            url="https://api.test.com",
            json_path="data.events"
        )
        
        with caplog.at_level(logging.ERROR):
            result = extractor.extract()
        
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        assert "Erro de estrutura no JSON" in caplog.text
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_partial_invalid_json_path(self, mock_get, mock_response_success, caplog):
        """Testa quando parte do path existe mas não é o esperado."""
        import logging
        
        mock_response_success.json.return_value = {"data": "not_a_dict"}
        mock_get.return_value = mock_response_success
        
        extractor = APIExtractor(
            endpoint_name="partial_path",
            url="https://api.test.com",
            json_path="data.events"
        )
        
        with caplog.at_level(logging.ERROR):
            result = extractor.extract()
        
        assert result.empty
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_complex_nested_data(self, mock_get, mock_response_success):
        """Testa normalização de dados complexos aninhados."""
        complex_data = [
            {
                "id": "1",
                "name": "Test",
                "links": {
                    "patch": {"small": "url1", "large": "url2"},
                    "reddit": {"campaign": None}
                },
                "cores": [
                    {"core": "core1", "flight": 1},
                    {"core": "core2", "flight": 2}
                ]
            }
        ]
        mock_response_success.json.return_value = complex_data
        mock_get.return_value = mock_response_success
        
        extractor = APIExtractor(
            endpoint_name="complex_data",
            url="https://api.test.com"
        )
        
        result = extractor.extract()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "id" in result.columns
        assert "name" in result.columns
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extract_unicode_content(self, mock_get, mock_response_success):
        """Testa extração com conteúdo unicode/caracteres especiais."""
        unicode_data = [
            {"id": "1", "name": "FalconSat 🚀", "description": "Café na órbita"},
            {"id": "2", "name": "DemoSat 日本語", "description": "测试"}
        ]
        mock_response_success.json.return_value = unicode_data
        mock_get.return_value = mock_response_success
        
        extractor = APIExtractor(
            endpoint_name="unicode_test",
            url="https://api.test.com"
        )
        
        result = extractor.extract()
        
        assert len(result) == 2
        assert "🚀" in result.iloc[0]["name"]
        assert "日本語" in result.iloc[1]["name"]


# =============================================================================
# CLASSE: TestAPIExtractorIntegrationStyle
# =============================================================================

class TestAPIExtractorIntegrationStyle:
    """Testes de estilo integração usando múltiplos mocks."""
    
    def test_multiple_extractors_independent_sessions(self):
        """Testa que cada extractor tem sua própria sessão."""
        extractor1 = APIExtractor(
            endpoint_name="api1",
            url="https://api1.com"
        )
        extractor2 = APIExtractor(
            endpoint_name="api2",
            url="https://api2.com"
        )
        
        assert extractor1.session is not extractor2.session
        assert extractor1.endpoint_name != extractor2.endpoint_name
    
    @patch('src.extractors.concrete_extractors.requests.Session.get')
    def test_extractor_reusability(self, mock_get, sample_spacex_launches, mock_response_success):
        """Testa que o mesmo extractor pode ser usado múltiplas vezes."""
        mock_response_success.json.return_value = sample_spacex_launches
        mock_get.return_value = mock_response_success
        
        extractor = APIExtractor(
            endpoint_name="reusable",
            url="https://api.test.com"
        )
        
        result1 = extractor.extract()
        assert len(result1) == 3
        
        result2 = extractor.extract()
        assert len(result2) == 3
        
        assert mock_get.call_count == 2