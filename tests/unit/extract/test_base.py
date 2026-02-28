"""
Testes para BaseExtractor.
Verifica validação de dados e integração com cliente.
"""

import pytest
from unittest import mock
from pydantic import BaseModel
from src.extract.base import BaseExtractor
from src.extract.client import SpaceXAPIClient


class MockSchema(BaseModel):
    """Schema fake para testes."""
    id: int
    name: str


class TestBaseExtractor:
    """Testes unitários para extração base."""
    
    @pytest.fixture
    def mock_client(self):
        """Cliente mockado."""
        client = mock.Mock(spec=SpaceXAPIClient)
        return client
    
    @pytest.fixture
    def extractor(self, mock_client):
        """Extractor com cliente mockado."""
        return BaseExtractor(client=mock_client)
    
    def test_init_with_custom_client(self, mock_client):
        """Deve aceitar cliente customizado."""
        extractor = BaseExtractor(client=mock_client)
        assert extractor.client is mock_client
    
    def test_init_with_default_client(self):
        """Deve criar cliente padrão se não fornecido."""
        with mock.patch('src.extract.base.SpaceXAPIClient') as mock_client_class:
            mock_instance = mock.Mock()
            mock_client_class.return_value = mock_instance
            
            extractor = BaseExtractor()
            assert extractor.client is mock_instance
    
    def test_validate_with_valid_data(self, extractor):
        """Deve validar dados corretos."""
        data = [{"id": 1, "name": "Falcon 9"}]
        
        result = extractor.validate(data, MockSchema)
        
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["name"] == "Falcon 9"
    
    def test_validate_skips_invalid_records(self, extractor):
        """Deve pular registros inválidos."""
        data = [
            {"id": 1, "name": "Valid"},
            {"id": "invalid", "name": 123},  # Tipos errados
            {"id": 2, "name": "Also Valid"}
        ]
        
        result = extractor.validate(data, MockSchema)
        
        # Deve manter apenas os válidos (id deve ser int)
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["id"] == 2
    
    def test_validate_without_schema(self, extractor):
        """Sem schema, deve retornar dados brutos."""
        data = [{"any": "data", "without": "schema"}]
        
        result = extractor.validate(data, None)
        
        assert result == data
    
    def test_validate_empty_list(self, extractor):
        """Lista vazia deve retornar vazia."""
        result = extractor.validate([], MockSchema)
        assert result == []
    
    def test_fetch_delegates_to_client_and_validates(self, extractor, mock_client):
        """Deve delegar fetch ao cliente e validar com schema."""
        # Dados completos compatíveis com RocketAPI
        mock_data = [
            {
                "id": "rocket1",
                "name": "Falcon 9",
                "active": True,
                "cost_per_launch": 50000000,
                "success_rate_pct": 98.5
            }
        ]
        mock_client.get.return_value = mock_data
        
        result = extractor.fetch("rockets")
        
        mock_client.get.assert_called_once_with("rockets")
        # Deve retornar dados validados (dict, não objeto Pydantic)
        assert len(result) == 1
        assert result[0]["id"] == "rocket1"
        assert result[0]["name"] == "Falcon 9"
    
    def test_fetch_with_endpoint_not_in_schemas(self, extractor, mock_client):
        """Endpoint sem schema deve retornar dados brutos."""
        mock_data = [{"raw": "data"}]
        mock_client.get.return_value = mock_data
        
        result = extractor.fetch("unknown_endpoint")
        
        # Sem schema no API_SCHEMAS, retorna dados crus
        assert result == mock_data
    
    def test_fetch_filters_invalid_data(self, extractor, mock_client):
        """Dados inválidos para o schema devem ser filtrados."""
        # Um válido, um inválido (falta campos obrigatórios)
        mock_data = [
            {
                "id": "rocket1",
                "name": "Falcon 9",
                "active": True,  # Completo
            },
            {
                "id": "rocket2",
                "name": "Falcon Heavy",
                # Falta active (obrigatório!)
            },
        ]
        mock_client.get.return_value = mock_data
        
        result = extractor.fetch("rockets")
        
        # Apenas o primeiro é válido
        assert len(result) == 1
        assert result[0]["id"] == "rocket1"
    
    def test_fetch_all_invalid_returns_empty(self, extractor, mock_client):
        """Se todos os dados forem inválidos, retorna lista vazia."""
        mock_data = [
            {"id": "rocket1"},  # Inválido
            {"id": "rocket2"},  # Inválido
        ]
        mock_client.get.return_value = mock_data
        
        result = extractor.fetch("rockets")
        
        assert result == []
    
    def test_fetch_validates_with_correct_schema(self, extractor, mock_client):
        """Deve usar schema correto baseado no endpoint."""
        # Testa launches
        launch_data = [
            {
                "id": "launch1",
                "name": "Starlink 1",
                "date_utc": "2024-01-01T00:00:00Z",
                "success": True,
                "rocket": "rocket1"
            }
        ]
        mock_client.get.return_value = launch_data
        
        result = extractor.fetch("launches")
        
        assert len(result) == 1
        assert result[0]["rocket"] == "rocket1"