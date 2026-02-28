# tests/unit/extract/test_factory.py
"""
Testes para factory de extractors.
Verifica padrão Factory Method para criação de extractors.
"""

import pytest
from unittest import mock

from src.extract.factory import get_extractor, EXTRACTORS
from src.extract.launches import LaunchExtract
from src.extract.rockets import RocketExtract
from src.extract.base import BaseExtractor


class TestGetExtractor:
    """Testes para factory de extractors."""
    
    def test_returns_rocket_extractor_class(self):
        """Deve retornar classe RocketExtract para 'rockets'."""
        result = get_extractor("rockets")
        
        assert result is RocketExtract
        assert isinstance(result, type)
        assert issubclass(result, BaseExtractor)
    
    def test_returns_launch_extractor_class(self):
        """Deve retornar classe LaunchExtract para 'launches'."""
        result = get_extractor("launches")
        
        assert result is LaunchExtract
        assert isinstance(result, type)
        assert issubclass(result, BaseExtractor)
    
    def test_raises_error_for_unknown_endpoint(self):
        """Deve lançar ValueError para endpoint desconhecido."""
        with pytest.raises(ValueError, match="Extractor para endpoint 'unknown' não encontrado"):
            get_extractor("unknown")
    
    def test_raises_error_for_empty_string(self):
        """Deve lançar ValueError para string vazia."""
        with pytest.raises(ValueError, match="Extractor para endpoint '' não encontrado"):
            get_extractor("")
    
    def test_extractors_registry_is_dict(self):
        """Registry deve ser um dicionário."""
        assert isinstance(EXTRACTORS, dict)
        assert "rockets" in EXTRACTORS
        assert "launches" in EXTRACTORS
    
    def test_returned_class_can_be_instantiated(self):
        """Classe retornada deve ser instanciável."""
        RocketClass = get_extractor("rockets")
        
        # Mockar o cliente no BaseExtractor (classe pai)
        with mock.patch('src.extract.base.SpaceXAPIClient'):
            instance = RocketClass()
            assert isinstance(instance, BaseExtractor)
    
    def test_all_registered_extractors_are_valid(self):
        """Todos os extractors registrados devem ser classes válidas."""
        for endpoint, extractor_class in EXTRACTORS.items():
            # Verifica se é uma classe
            assert isinstance(extractor_class, type), \
                f"{endpoint} não é uma classe"
            
            # Verifica se herda de BaseExtractor
            assert issubclass(extractor_class, BaseExtractor), \
                f"{endpoint} não herda de BaseExtractor"
    
    def test_case_sensitive_endpoints(self):
        """Endpoints devem ser case-sensitive."""
        with pytest.raises(ValueError):
            get_extractor("Rockets")  # Maiúsculo
        
        with pytest.raises(ValueError):
            get_extractor("ROCKETS")  # Uppercase
        
        with pytest.raises(ValueError):
            get_extractor("Launches")  # Capitalizado