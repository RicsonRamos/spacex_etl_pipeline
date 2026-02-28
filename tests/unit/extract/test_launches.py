# tests/unit/extract/test_launches.py
"""
Testes para LaunchExtract.
Verifica extração de dados de lançamentos (real e mock).
"""

import pytest
from unittest import mock

from src.extract.launches import LaunchExtract, DEFAULT_MOCK_LAUNCHES


class TestLaunchExtract:
    """Testes para extração de dados de lançamentos."""
    
    @pytest.fixture
    def extractor(self):
        """Extractor com cliente mockado."""
        with mock.patch('src.extract.base.SpaceXAPIClient') as mock_client:
            instance = mock.Mock()
            mock_client.return_value = instance
            yield LaunchExtract()
    
    def test_endpoint_class_attribute(self):
        """Deve ter endpoint definido como classe."""
        assert LaunchExtract.endpoint == "launches"
    
    def test_extract_with_mock_data(self, extractor):
        """Deve retornar dados mock quando real_api=False."""
        result = extractor.extract(real_api=False)
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["rocket"] == "falcon9"
        assert result[1]["name"] == "Starlink 2"
        assert result == DEFAULT_MOCK_LAUNCHES
    
    def test_extract_with_real_api(self, extractor):
        """Deve chamar API quando real_api=True."""
        api_data = [{"id": "launch3", "name": "Crew Dragon"}]
        
        with mock.patch.object(extractor, 'fetch', return_value=api_data) as mock_fetch:
            result = extractor.extract(real_api=True)
            
            mock_fetch.assert_called_once_with("launches")
            assert result == api_data
    
    def test_extract_returns_list(self, extractor):
        """Resultado deve sempre ser uma lista."""
        result = extractor.extract(real_api=False)
        assert isinstance(result, list)
    
    def test_extract_handles_empty_mock(self, extractor):
        """Mock padrão não está vazio."""
        result = extractor.extract(real_api=False)
        assert len(result) > 0  # Tem dados padrão
    
    def test_inherits_from_base_extractor(self):
        """Deve herdar de BaseExtractor."""
        from src.extract.base import BaseExtractor
        assert issubclass(LaunchExtract, BaseExtractor)
    
    def test_has_extract_method(self, extractor):
        """Deve ter método extract."""
        assert hasattr(extractor, 'extract')
        assert callable(extractor.extract)
    
    def test_default_mock_launches_structure(self):
        """Mock padrão deve ter estrutura correta."""
        for launch in DEFAULT_MOCK_LAUNCHES:
            assert "id" in launch
            assert "name" in launch
            assert "date_utc" in launch
            assert "rocket" in launch