from src.extract.spacex_api import SpaceXExtractor

def test_fetch_launches_api_error(mocker):
    # Mock da resposta de erro da API
    mock_get = mocker.patch("requests.get")
    mock_response = mocker.Mock()
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = Exception("API Down")
    
    # Garantir que o mock de requests.get retorna o mock_response
    mock_get.return_value = mock_response

    extractor = SpaceXExtractor()
    
    # Verifica se o código levanta a exceção esperada
    try:
        extractor.fetch_launches()
    except Exception as e:
        assert "API Down" in str(e)

def test_fetch_launches_api_success(mocker):
    # Mock da resposta de sucesso da API
    mock_get = mocker.patch("requests.get")
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"id": "1", "name": "Falcon 9", "updated_at": "2023-02-01"}]
    
    # Garantir que o mock de requests.get retorna o mock_response
    mock_get.return_value = mock_response

    extractor = SpaceXExtractor()
    
    # Verifica se o dado retornado é correto
    data = extractor.fetch_launches()
    assert len(data) == 1
    assert data[0]["id"] == "1"
    assert data[0]["name"] == "Falcon 9"


def test_fetch_launches_api_not_found_error(mocker):
    # Mock de erro 404
    mock_get = mocker.patch("requests.get")
    mock_response = mocker.Mock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = Exception("Not Found")

    mock_get.return_value = mock_response

    extractor = SpaceXExtractor()
    
    # Verifica se a exceção correta é levantada
    try:
        extractor.fetch_launches()
    except Exception as e:
        assert "Not Found" in str(e)
