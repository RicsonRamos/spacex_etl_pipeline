from src.extract.extractor import SpaceXExtractor

def test_fetch_launches_api_error(mocker):
    # Mock da resposta de erro da API
    mock_get = mocker.patch("requests.get")
    mock_get.return_value.status_code = 500
    mock_get.return_value.raise_for_status.side_effect = Exception("API Down")

    extractor = SpaceXExtractor()
    
    # Verifica se o código levanta a exceção esperada
    try:
        extractor.fetch_launches()
    except Exception as e:
        assert "API Down" in str(e)
