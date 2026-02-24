import pytest
from src.extract.spacex_api import SpaceXExtractor
from src.extract.schemas import API_SCHEMAS
import requests


# Função auxiliar para criar mock de resposta
def make_mock_response(mocker, json_data=None, raise_exc=None):
    mock_resp = mocker.Mock()
    if raise_exc:
        mock_resp.raise_for_status.side_effect = raise_exc
    else:
        mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = json_data or []
    return mock_resp



# Mock completo para LaunchAPI

mock_launch = {
    "id": "1",
    "name": "Falcon 9",
    "date_utc": "2023-02-01T00:00:00Z",
    "rocket": "falcon9",
    "success": True,
    "details": "Teste lançamento",
    "flight_number": 123,
    "upcoming": False,
    "cores": [{"core": "5e9e289df35918033d3b2623", "flight": 1}],
    "payloads": ["5eb0e4b5b6c3bb0006eeb1e1"],
    "launchpad": "5e9e4502f5090995de566f86"
}


# Mock completo para RocketAPI

mock_rocket = {
    "id": "falcon9",
    "name": "Falcon 9",
    "active": True,
    "stages": 2,
    "boosters": 0,
    "cost_per_launch": 50000000,
    "success_rate_pct": 98,
    "first_flight": "2010-06-04",
    "country": "USA",
    "company": "SpaceX",
    "height": {"meters": 70, "feet": 229.6},
    "diameter": {"meters": 3.7, "feet": 12.1},
    "mass": {"kg": 549054, "lb": 1207920}
}



# Testes


def test_fetch_success_with_valid_schema(mocker):
    mock_session = mocker.Mock()
    mock_session.get.return_value = make_mock_response(mocker, json_data=[mock_launch])

    extractor = SpaceXExtractor(session=mock_session)
    result = extractor.fetch("launches")

    assert len(result) == 1
    assert result[0]["id"] == "1"
    assert result[0]["name"] == "Falcon 9"


def test_fetch_success_with_no_schema(mocker):
    endpoint = "unknown_endpoint"
    mock_data = [{"id": "123", "foo": "bar"}]

    mock_session = mocker.Mock()
    mock_session.get.return_value = make_mock_response(mocker, json_data=mock_data)

    extractor = SpaceXExtractor(session=mock_session)
    result = extractor.fetch(endpoint)

    # Deve retornar os dados brutos, porque não existe schema
    assert result == mock_data


def test_fetch_with_invalid_records(mocker):
    endpoint = "launches"
    # 1 registro válido + 1 registro inválido (vazio)
    mock_data = [mock_launch.copy(), {}]

    mock_session = mocker.Mock()
    mock_session.get.return_value = make_mock_response(mocker, json_data=mock_data)

    extractor = SpaceXExtractor(session=mock_session)
    result = extractor.fetch(endpoint)

    # Apenas o primeiro registro válido deve ser retornado
    assert len(result) == 1
    assert result[0]["id"] == "1"


def test_fetch_http_error(mocker):
    mock_session = mocker.Mock()
    mock_session.get.return_value = make_mock_response(
        mocker, raise_exc=requests.RequestException("API Error")
    )

    extractor = SpaceXExtractor(session=mock_session)
    with pytest.raises(requests.RequestException) as excinfo:
        extractor.fetch("launches")

    assert "API Error" in str(excinfo.value)


def test_fetch_launches_and_rockets_methods(mocker):
    # Mock de cada endpoint
    mock_session = mocker.Mock()

    def side_effect(url, timeout):
        if "launches" in url:
            return make_mock_response(mocker, json_data=[mock_launch])
        elif "rockets" in url:
            return make_mock_response(mocker, json_data=[mock_rocket])
        return make_mock_response(mocker, json_data=[])

    mock_session.get.side_effect = side_effect

    extractor = SpaceXExtractor(session=mock_session)

    launches = extractor.fetch_launches()
    rockets = extractor.fetch_rockets()

    assert len(launches) == 1
    assert launches[0]["id"] == "1"

    assert len(rockets) == 1
    assert rockets[0]["id"] == "falcon9"