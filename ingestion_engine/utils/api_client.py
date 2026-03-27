"""
Utilitários para testes de API clients.
Fornece mocks, fixtures e helpers para simular comportamentos de APIs externas.
"""

import json
import pandas as pd
from unittest.mock import Mock
from typing import Dict, List, Any, Optional, Union


class MockAPIResponse:
    """
    Factory para criar mocks de respostas HTTP para testes.
    Simula comportamentos da biblioteca requests.Response.
    """
    
    @staticmethod
    def success(
        data: Union[Dict, List] = None,
        headers: Optional[Dict] = None,
        status_code: int = 200
    ) -> Mock:
        """
        Cria um mock de resposta HTTP bem-sucedida.
        
        Args:
            data: Dados JSON de retorno
            headers: Headers HTTP (default: Content-Type: application/json)
            status_code: Código HTTP (default: 200)
        
        Returns:
            Mock configurado como requests.Response
        """
        if data is None:
            data = []
        
        if headers is None:
            headers = {
                "Content-Type": "application/json",
                "X-RateLimit-Remaining": "100"
            }
        
        mock_response = Mock()
        mock_response.status_code = status_code
        mock_response.headers = headers
        mock_response.json.return_value = data
        mock_response.raise_for_status.return_value = None
        mock_response.text = json.dumps(data)
        mock_response.content = json.dumps(data).encode('utf-8')
        
        return mock_response
    
    @staticmethod
    def rate_limit_critical(
        data: Union[Dict, List] = None,
        remaining: int = 3
    ) -> Mock:
        """
        Cria mock de resposta com rate limit crítico (poucas requisições restantes).
        
        Args:
            data: Dados JSON de retorno
            remaining: Número de requisições restantes (default: 3)
        """
        headers = {
            "Content-Type": "application/json",
            "X-RateLimit-Remaining": str(remaining),
            "X-RateLimit-Limit": "100"
        }
        return MockAPIResponse.success(data=data, headers=headers)
    
    @staticmethod
    def error(
        status_code: int = 500,
        error_message: str = "Internal Server Error"
    ) -> Mock:
        """
        Cria mock de resposta HTTP com erro.
        
        Args:
            status_code: Código de erro HTTP
            error_message: Mensagem de erro
        
        Returns:
            Mock configurado para lançar exceção em raise_for_status()
        """
        from requests import HTTPError
        
        mock_response = Mock()
        mock_response.status_code = status_code
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.text = json.dumps({"error": error_message})
        
        # Configura raise_for_status para lançar exceção
        http_error = HTTPError(f"{status_code} Error: {error_message}")
        http_error.response = mock_response
        mock_response.raise_for_status.side_effect = http_error
        
        return mock_response
    
    @staticmethod
    def unauthorized() -> Mock:
        """Mock específico para erro 401 - Unauthorized"""
        return MockAPIResponse.error(
            status_code=401,
            error_message="Unauthorized - Invalid API Key"
        )
    
    @staticmethod
    def forbidden() -> Mock:
        """Mock específico para erro 403 - Forbidden (NASA API Key inválida)"""
        return MockAPIResponse.error(
            status_code=403,
            error_message="Forbidden - Invalid API Key"
        )
    
    @staticmethod
    def not_found() -> Mock:
        """Mock específico para erro 404 - Not Found"""
        return MockAPIResponse.error(
            status_code=404,
            error_message="Not Found"
        )
    
    @staticmethod
    def server_error() -> Mock:
        """Mock específico para erro 500 - Internal Server Error"""
        return MockAPIResponse.error(
            status_code=500,
            error_message="Internal Server Error"
        )
    
    @staticmethod
    def service_unavailable() -> Mock:
        """Mock específico para erro 503 - Service Unavailable"""
        return MockAPIResponse.error(
            status_code=503,
            error_message="Service Unavailable"
        )


class SpaceXAPIMocker:
    """
    Helper para criar mocks específicos da API SpaceX.
    """
    
    @staticmethod
    def get_sample_launch(
        launch_id: str = "5eb87cd9ffd86e000604b32a",
        flight_number: int = 1,
        name: str = "FalconSat",
        success: bool = False
    ) -> Dict[str, Any]:
        """
        Gera um lançamento SpaceX de exemplo.
        
        Args:
            launch_id: ID único do lançamento
            flight_number: Número do voo
            name: Nome da missão
            success: Se foi bem-sucedido
        
        Returns:
            Dict com estrutura de lançamento SpaceX
        """
        return {
            "id": launch_id,
            "flight_number": flight_number,
            "name": name,
            "date_utc": "2006-03-24T22:30:00.000Z",
            "date_unix": 1143239400,
            "date_local": "2006-03-25T10:30:00+12:00",
            "date_precision": "hour",
            "static_fire_date_utc": "2006-03-17T00:00:00.000Z",
            "static_fire_date_unix": 1142553600,
            "tdb": False,
            "net": False,
            "window": 0,
            "rocket": "5e9d0d95eda69955f709d1eb",
            "success": success,
            "failures": [],
            "upcoming": False,
            "details": "Engine failure at 33 seconds and loss of vehicle",
            "crew": [],
            "ships": [],
            "capsules": [],
            "payloads": ["5eb0e4b5b6c3bb0006eeb1e1"],
            "launchpad": "5e9e4502f5090995de566f86",
            "flight_number": flight_number,
            "name": name,
            "links": {
                "patch": {
                    "small": "https://images2.imgbox.com/3c/0e/T8iJcSN3_o.png",
                    "large": "https://images2.imgbox.com/40/e3/GypSkayF_o.png"
                },
                "reddit": {
                    "campaign": None,
                    "launch": None,
                    "media": None,
                    "recovery": None
                },
                "flickr": {
                    "small": [],
                    "original": []
                },
                "presskit": None,
                "webcast": "https://www.youtube.com/watch?v=0a_00nJ_Y88",
                "youtube_id": "0a_00nJ_Y88",
                "article": "https://www.space.com/2196-spacex-inaugural-falcon-1-rocket-lost-launch.html",
                "wikipedia": "https://en.wikipedia.org/wiki/DemoSat"
            },
            "auto_update": True
        }
    
    @staticmethod
    def get_sample_launches(count: int = 2) -> List[Dict[str, Any]]:
        """
        Gera uma lista de lançamentos SpaceX de exemplo.
        
        Args:
            count: Quantidade de lançamentos (default: 2)
        
        Returns:
            Lista de dicts com lançamentos
        """
        launches = []
        for i in range(count):
            launches.append(
                SpaceXAPIMocker.get_sample_launch(
                    launch_id=f"5eb87cd9ffd86e000604b32{i}",
                    flight_number=i + 1,
                    name=f"Launch {i + 1}",
                    success=(i % 2 == 0)  # Alterna entre True e False
                )
            )
        return launches
    
    @staticmethod
    def get_empty_response() -> List:
        """Retorna resposta vazia da API"""
        return []
    
    @staticmethod
    def get_rocket_sample(rocket_id: str = "5e9d0d95eda69955f709d1eb") -> Dict[str, Any]:
        """
        Gera dados de exemplo de um foguete.
        
        Args:
            rocket_id: ID do foguete
        
        Returns:
            Dict com dados do foguete
        """
        return {
            "id": rocket_id,
            "name": "Falcon 1",
            "type": "rocket",
            "active": False,
            "stages": 2,
            "boosters": 0,
            "cost_per_launch": 6700000,
            "success_rate_pct": 40,
            "first_flight": "2006-03-24",
            "country": "Republic of the Marshall Islands",
            "company": "SpaceX",
            "height": {"meters": 22.25, "feet": 73},
            "diameter": {"meters": 1.68, "feet": 5.5},
            "mass": {"kg": 30146, "lb": 66460}
        }


class NASAAPIMocker:
    """
    Helper para criar mocks específicos da API NASA (DONKI).
    """
    
    @staticmethod
    def get_sample_solar_event(
        activity_id: str = "2026-03-27T12:00:00-SEP-001",
        event_type: str = "SEP"
    ) -> Dict[str, Any]:
        """
        Gera um evento solar de exemplo.
        
        Args:
            activity_id: ID da atividade
            event_type: Tipo de evento (SEP, FLR, CME, etc.)
        
        Returns:
            Dict com estrutura de evento solar NASA
        """
        return {
            "activityID": activity_id,
            "startTime": "2026-03-27T12:00:00Z",
            "peakTime": "2026-03-27T14:30:00Z",
            "endTime": "2026-03-27T18:00:00Z",
            "eventType": event_type,
            "classType": "X1.0" if event_type == "FLR" else None,
            "sourceLocation": "N20E15",
            "activeRegionNum": 13000,
            "link": f"https://kauai.ccmc.gsfc.nasa.gov/DONKI/view/{event_type}/1/",
            "note": f"Sample {event_type} event for testing"
        }
    
    @staticmethod
    def get_sample_events(count: int = 2, event_type: str = "SEP") -> List[Dict[str, Any]]:
        """
        Gera uma lista de eventos solares.
        
        Args:
            count: Quantidade de eventos
            event_type: Tipo de evento
        
        Returns:
            Lista de eventos
        """
        events = []
        for i in range(count):
            events.append(
                NASAAPIMocker.get_sample_solar_event(
                    activity_id=f"2026-03-27T12:00:00-{event_type}-{i:03d}",
                    event_type=event_type
                )
            )
        return events
    
    @staticmethod
    def get_apod_sample() -> Dict[str, Any]:
        """
        Gera dados de exemplo da API APOD (Astronomy Picture of the Day).
        
        Returns:
            Dict com dados APOD
        """
        return {
            "date": "2026-03-27",
            "explanation": "This is a sample explanation for testing purposes.",
            "hdurl": "https://apod.nasa.gov/apod/image/2603/sample_hd.jpg",
            "media_type": "image",
            "service_version": "v1",
            "title": "Sample Astronomy Picture",
            "url": "https://apod.nasa.gov/apod/image/2603/sample.jpg"
        }
    
    @staticmethod
    def get_nested_response(data_key: str = "data") -> Dict[str, Any]:
        """
        Gera resposta aninhada típica de algumas APIs NASA.
        
        Args:
            data_key: Chave onde os dados estão aninhados
        
        Returns:
            Dict com estrutura aninhada
        """
        return {
            data_key: {
                "events": NASAAPIMocker.get_sample_events(count=3),
                "count": 3,
                "page": 1
            },
            "meta": {
                "total": 3,
                "limit": 10
            }
        }


class DataFrameAssertions:
    """
    Helpers para asserções em DataFrames pandas durante testes.
    """
    
    @staticmethod
    def assert_has_columns(df: pd.DataFrame, columns: List[str]) -> None:
        """
        Assertiva: DataFrame deve conter todas as colunas especificadas.
        
        Args:
            df: DataFrame a verificar
            columns: Lista de nomes de colunas esperadas
        
        Raises:
            AssertionError: Se alguma coluna estiver faltando
        """
        missing = [col for col in columns if col not in df.columns]
        if missing:
            raise AssertionError(f"Colunas ausentes: {missing}. "
                               f"Colunas presentes: {list(df.columns)}")
    
    @staticmethod
    def assert_no_nulls_in_column(df: pd.DataFrame, column: str) -> None:
        """
        Assertiva: Coluna não deve ter valores nulos.
        
        Args:
            df: DataFrame a verificar
            column: Nome da coluna
        
        Raises:
            AssertionError: Se houver valores nulos
        """
        null_count = df[column].isnull().sum()
        if null_count > 0:
            raise AssertionError(f"Coluna '{column}' tem {null_count} valores nulos")
    
    @staticmethod
    def assert_column_type(df: pd.DataFrame, column: str, expected_type: type) -> None:
        """
        Assertiva: Coluna deve ter tipo específico.
        
        Args:
            df: DataFrame a verificar
            column: Nome da coluna
            expected_type: Tipo esperado (str, int, float, etc.)
        
        Raises:
            AssertionError: Se o tipo não corresponder
        """
        if column not in df.columns:
            raise AssertionError(f"Coluna '{column}' não existe")
        
        actual_type = df[column].dtype
        # Verificação simplificada para tipos
        type_mapping = {
            str: ['object', 'string'],
            int: ['int64', 'int32', 'int'],
            float: ['float64', 'float32', 'float'],
            bool: ['bool']
        }
        
        if expected_type in type_mapping:
            if str(actual_type) not in type_mapping[expected_type]:
                raise AssertionError(
                    f"Coluna '{column}' tem tipo {actual_type}, "
                    f"esperado {expected_type}"
                )
    
    @staticmethod
    def assert_row_count(df: pd.DataFrame, expected_count: int) -> None:
        """
        Assertiva: DataFrame deve ter número específico de linhas.
        
        Args:
            df: DataFrame a verificar
            expected_count: Número esperado de linhas
        
        Raises:
            AssertionError: Se a contagem não corresponder
        """
        actual_count = len(df)
        if actual_count != expected_count:
            raise AssertionError(
                f"Esperado {expected_count} linhas, encontrado {actual_count}"
            )


class SessionMocker:
    """
    Helper para mockar sessões requests.Session de forma mais realista.
    """
    
    @staticmethod
    def create_mock_session(
        responses: List[Mock],
        raise_on_call: Optional[int] = None
    ) -> Mock:
        """
        Cria uma sessão mock que retorna respostas sequenciais.
        
        Args:
            responses: Lista de mocks de resposta para retornar em sequência
            raise_on_call: Índice da chamada que deve lançar exceção (opcional)
        
        Returns:
            Mock configurado como requests.Session
        """
        mock_session = Mock()
        call_count = [0]  # Lista para mutabilidade em closure
        
        def mock_get(*args, **kwargs):
            current = call_count[0]
            call_count[0] += 1
            
            if raise_on_call is not None and current == raise_on_call:
                raise Exception("Simulated connection error")
            
            if current < len(responses):
                return responses[current]
            return responses[-1] if responses else Mock()
        
        mock_session.get = mock_get
        mock_session.mount = Mock()  # Para HTTPAdapter
        
        return mock_session
    
    @staticmethod
    def assert_session_calls(
        mock_session: Mock,
        expected_urls: List[str],
        expected_params: Optional[List[Dict]] = None
    ) -> None:
        """
        Verifica se a sessão foi chamada com URLs e parâmetros esperados.
        
        Args:
            mock_session: Mock da sessão
            expected_urls: Lista de URLs esperadas
            expected_params: Lista de parâmetros esperados (opcional)
        """
        calls = mock_session.get.call_args_list
        
        assert len(calls) == len(expected_urls), \
            f"Esperado {len(expected_urls)} chamadas, obtido {len(calls)}"
        
        for i, (call, expected_url) in enumerate(zip(calls, expected_urls)):
            actual_url = call[0][0] if call[0] else call[1].get('url')
            assert actual_url == expected_url, \
                f"Chamada {i}: URL esperada {expected_url}, obtida {actual_url}"
        
        if expected_params:
            for i, (call, expected_param) in enumerate(zip(calls, expected_params)):
                actual_params = call[1].get('params') if len(call) > 1 else None
                assert actual_params == expected_param, \
                    f"Chamada {i}: Params esperados {expected_param}, obtidos {actual_params}"


# Exemplo de uso (para documentação)
if __name__ == "__main__":
    # Demonstração dos helpers
    print("=== MockAPIResponse Examples ===")
    
    # Sucesso
    success = MockAPIResponse.success(
        data=[{"id": "1", "name": "Test"}],
        headers={"X-RateLimit-Remaining": "50"}
    )
    print(f"Success mock status: {success.status_code}")
    print(f"Success mock data: {success.json()}")
    
    # Erro
    error = MockAPIResponse.forbidden()
    print(f"\nError mock status: {error.status_code}")
    
    print("\n=== SpaceXAPIMocker Examples ===")
    launch = SpaceXAPIMocker.get_sample_launch()
    print(f"Sample launch: {launch['name']} (Flight {launch['flight_number']})")
    
    launches = SpaceXAPIMocker.get_sample_launches(count=3)
    print(f"Generated {len(launches)} launches")
    
    print("\n=== NASAAPIMocker Examples ===")
    event = NASAAPIMocker.get_sample_solar_event()
    print(f"Sample event: {event['activityID']} ({event['eventType']})")
    
    print("\n=== DataFrameAssertions Examples ===")
    df = pd.DataFrame({
        "id": ["1", "2", "3"],
        "name": ["A", "B", "C"],
        "value": [10.5, 20.5, 30.5]
    })
    
    try:
        DataFrameAssertions.assert_has_columns(df, ["id", "name"])
        print("✓ Columns assertion passed")
        
        DataFrameAssertions.assert_row_count(df, 3)
        print("✓ Row count assertion passed")
        
        DataFrameAssertions.assert_no_nulls_in_column(df, "id")
        print("✓ No nulls assertion passed")
    except AssertionError as e:
        print(f"✗ Assertion failed: {e}")