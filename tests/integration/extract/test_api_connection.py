import requests
import pytest

SPACE_X_BASE_URL = "https://api.spacexdata.com/v4" 

@pytest.mark.parametrize("endpoint", ["launches", "rockets"])
def test_api_endpoint_connection(endpoint):
    # Faz a requisição
    response = requests.get(f"{SPACE_X_BASE_URL}/{endpoint}")
    
    # Verifica se a requisição foi bem-sucedida
    assert response.status_code == 200, f"Falha na requisição: {response.status_code}"
    
    # Converte para JSON
    data = response.json()
    
    # Verifica se é uma lista
    assert isinstance(data, list), f"Erro: o retorno do endpoint '{endpoint}' não é uma lista"
    
    # Verifica se a lista não está vazia
    assert len(data) > 0, f"Erro: o endpoint '{endpoint}' retornou uma lista vazia"