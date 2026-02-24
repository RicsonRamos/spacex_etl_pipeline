from src.extract.spacex_api import SpaceXExtractor
import requests


def test_fetch_launches_api_error(mocker):
    mock_session = mocker.Mock()
    mock_response = mocker.Mock()
    mock_response.raise_for_status.side_effect = requests.RequestException("API Down")

    mock_session.get.return_value = mock_response

    extractor = SpaceXExtractor(session=mock_session)

    try:
        extractor.fetch_launches()
        assert False  # se não levantar erro, falhou
    except Exception as e:
        assert "API Down" in str(e)


def test_fetch_launches_api_success(mocker):
    mock_session = mocker.Mock()
    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = [
        {
            "id": "1",
            "name": "Falcon 9",
            "date_utc": "2023-02-01T00:00:00Z",
            "rocket": "falcon9"
        }
    ]
    mock_session.get.return_value = mock_response

    extractor = SpaceXExtractor(session=mock_session)

    data = extractor.fetch_launches()

    assert len(data) == 1
    assert data[0]["id"] == "1"
    assert data[0]["name"] == "Falcon 9"


def test_fetch_launches_api_not_found_error(mocker):
    mock_session = mocker.Mock()
    mock_response = mocker.Mock()
    mock_response.raise_for_status.side_effect = requests.RequestException("Not Found")

    mock_session.get.return_value = mock_response

    extractor = SpaceXExtractor(session=mock_session)

    try:
        extractor.fetch_launches()
        assert False
    except Exception as e:
        assert "Not Found" in str(e)