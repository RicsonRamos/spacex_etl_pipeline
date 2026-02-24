import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
import polars as pl
from src.transform.transformer import SpaceXTransformer
from src.config.schema_registry import SCHEMA_REGISTRY


# Fixtures

@pytest.fixture
def transformer():
    return SpaceXTransformer()

@pytest.fixture
def sample_raw_launch():
    return {
        "id": "5eb87cd9ffd86e000604b32a",
        "name": "FalconSat",
        "date_utc": "2006-03-24T22:30:00.000Z",
        "rocket": "5e9d0d95eda69955f709d1eb",
        "success": False,
        "details": "Engine failure at 33 seconds and loss of vehicle",
        "flight_number": 1,
        "links": {"patch": {"small": "https://images2.imgbox.com/3c/0e/T8iJcSN3_o.png"}},
    }

@pytest.fixture
def mock_schema_factory():
    """Factory para criar mocks de schema"""
    def _create(columns, pk="id", rename=None, casts=None):
        schema = MagicMock()
        schema.columns = columns
        schema.pk = pk
        schema.rename = rename or {}
        schema.casts = casts or {}
        return schema
    return _create


# Testes Básicos

def test_transform_launches_success(transformer, sample_raw_launch):
    """Testa transformação básica de launches"""
    raw_data = [sample_raw_launch]
    df = transformer.transform_launches(raw_data)

    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] == 1
    assert "launch_id" in df.columns
    assert df["launch_id"][0] == sample_raw_launch["id"]
    assert df["success"].dtype == pl.Boolean

def test_transform_empty_data(transformer):
    """Testa retorno de DataFrame vazio quando data é vazio"""
    result = transformer.transform("launches", [])
    assert isinstance(result, pl.DataFrame)
    assert result.is_empty()

def test_transform_endpoint_not_in_registry(transformer):
    """Testa erro quando endpoint não existe no registry"""
    with patch.dict(SCHEMA_REGISTRY, {}, clear=True):
        with pytest.raises(ValueError, match="Endpoint 'invalid_endpoint' não mapeado"):
            transformer.transform("invalid_endpoint", [{"id": 1}])


# Testes de process_in_batches

def test_process_in_batches(transformer):
    """Testa divisão de DataFrame em batches"""
    df = pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["A", "B", "C", "D", "E"]
    })
    
    batches = list(transformer.process_in_batches(df, batch_size=2))
    
    assert len(batches) == 3
    assert batches[0].shape == (2, 2)
    assert batches[1].shape == (2, 2)
    assert batches[2].shape == (1, 2)
    assert batches[0]["id"].to_list() == [1, 2]
    assert batches[2]["id"].to_list() == [5]


# Testes de Incremental / last_ingested

def test_transform_with_last_ingested_filter(transformer, sample_raw_launch):
    """Testa filtro incremental baseado em last_ingested"""
    old_launch = sample_raw_launch.copy()
    old_launch["id"] = "old-id"
    old_launch["date_utc"] = "2000-01-01T00:00:00.000Z"
    
    new_launch = sample_raw_launch.copy()
    new_launch["id"] = "new-id"
    new_launch["date_utc"] = "2023-01-01T00:00:00.000Z"
    
    raw_data = [old_launch, new_launch]
    last_ingested = datetime(2020, 1, 1, tzinfo=timezone.utc)
    
    df = transformer.transform("launches", raw_data, last_ingested=last_ingested)
    
    assert df.shape[0] == 1
    assert df["launch_id"][0] == "new-id"

def test_transform_with_last_ingested_no_tz(transformer, sample_raw_launch):
    """Testa que last_ingested sem timezone é convertido para UTC"""
    sample_raw_launch["date_utc"] = "2023-01-01T00:00:00.000Z"
    last_ingested = datetime(2020, 1, 1)  # Sem tzinfo
    
    df = transformer.transform("launches", [sample_raw_launch], last_ingested=last_ingested)
    assert isinstance(df, pl.DataFrame)




def test_transform_with_rename_and_casts(transformer, mock_schema_factory):
    """Testa rename de colunas e casting de tipos"""
   
    data = [{
        "id": "123",
        "name": "Test Launch",
        "date_utc": "2023-01-01T00:00:00.000Z",
        "success": True  
    }]
    
    mock_schema = mock_schema_factory(
        columns=["launch_id", "name", "date_utc", "success"],
        pk="launch_id",
        rename={"id": "launch_id"},
        casts={"success": pl.Boolean}
    )
    
    with patch.dict(SCHEMA_REGISTRY, {"launches": mock_schema}, clear=False):
        df = transformer.transform("launches", data)
        
        assert "id" not in df.columns
        assert "launch_id" in df.columns
        assert df["success"].dtype == pl.Boolean

def test_transform_rename_partial(transformer, mock_schema_factory):
    """Testa quando apenas algumas colunas do rename existem"""
    data = [{
        "id": "123",
        "name": "Test",
        "date_utc": "2023-01-01T00:00:00.000Z",
        "success": True
    }]
    
    
    mock_schema = mock_schema_factory(
        columns=["launch_id", "name", "date_utc", "success"], 
        pk="launch_id",
        rename={"id": "launch_id", "non_existent": "also_non_existent"},
        casts={}
    )
    
    with patch.dict(SCHEMA_REGISTRY, {"launches": mock_schema}, clear=False):
        df = transformer.transform("launches", data)
        assert "launch_id" in df.columns


# Testes de Deduplicação

def test_transform_deduplication_with_pk(transformer, mock_schema_factory):
    """Testa deduplicação baseada na chave primária"""
    data = [
        {"id": "1", "name": "First", "date_utc": "2023-01-01T00:00:00.000Z", "success": True},
        {"id": "1", "name": "Duplicate", "date_utc": "2023-01-02T00:00:00.000Z", "success": False},
        {"id": "2", "name": "Second", "date_utc": "2023-01-03T00:00:00.000Z", "success": True},
    ]
    
    mock_schema = mock_schema_factory(
        columns=["id", "name", "date_utc", "success"],
        pk="id"
    )
    
    with patch.dict(SCHEMA_REGISTRY, {"launches": mock_schema}, clear=False):
        df = transformer.transform("launches", data)
        assert df.shape[0] == 2
        assert df.filter(pl.col("id").is_null()).is_empty()

def test_transform_deduplication_pk_null(transformer, mock_schema_factory):
    """Testa que registros com PK nulo são removidos"""
    data = [
        {"id": None, "name": "Null ID", "date_utc": "2023-01-01T00:00:00.000Z", "success": True},
        {"id": "1", "name": "Valid", "date_utc": "2023-01-02T00:00:00.000Z", "success": True},
    ]
    
    mock_schema = mock_schema_factory(
        columns=["id", "name", "date_utc", "success"],
        pk="id"
    )
    
    with patch.dict(SCHEMA_REGISTRY, {"launches": mock_schema}, clear=False):
        df = transformer.transform("launches", data)
        assert df.shape[0] == 1
        assert df["id"][0] == "1"


# Testes de Schema Validation

def test_transform_missing_columns_raises_error(transformer, mock_schema_factory):
    """Testa erro quando colunas do schema não estão presentes"""
    data = [{"id": "1", "name": "Test"}]  # Faltando date_utc e success
    
    mock_schema = mock_schema_factory(
        columns=["id", "name", "date_utc", "success", "extra_col"]  # extra_col não existe
    )
    
    with patch.dict(SCHEMA_REGISTRY, {"launches": mock_schema}, clear=False):
        with pytest.raises(ValueError, match="Divergência de Schema"):
            transformer.transform("launches", data)


# Testes de Tratamento de Erro

def test_transform_exception_propagation(transformer):
    """Testa que exceções são propagadas após logging"""
    data = [{"id": "1"}]
    
    with patch.dict(SCHEMA_REGISTRY, {}, clear=True):
        with pytest.raises(ValueError, match="não mapeado"):
            transformer.transform("launches", data)

def test_transform_logs_exception_on_error(transformer):
    """Testa que exceções são logadas e propagadas"""
    data = [{"id": "1"}]
    
    with patch.dict(SCHEMA_REGISTRY, {}, clear=True):
        with pytest.raises(ValueError):
            transformer.transform("launches", data)


# Testes de Date Parsing Edge Cases

def test_transform_date_parsing_various_formats(transformer, mock_schema_factory):
    """Testa parsing de datas em diferentes formatos ISO"""
    data = [
        {"id": "1", "date_utc": "2023-01-01T00:00:00Z", "success": True},
        {"id": "2", "date_utc": "2023-06-15T10:30:45.123456Z", "success": True},
    ]
    
    mock_schema = mock_schema_factory(
        columns=["id", "date_utc", "success"],
        pk="id"
    )
    
    with patch.dict(SCHEMA_REGISTRY, {"launches": mock_schema}, clear=False):
        df = transformer.transform("launches", data)
        assert df.shape[0] == 2
        assert df["date_utc"].dtype == pl.Datetime

def test_transform_date_already_datetime(transformer, mock_schema_factory):
    """Testa quando a coluna de data já é do tipo datetime"""
    data = [{"id": "1", "date_utc": "2023-01-01T00:00:00Z", "success": True}]
    
    mock_schema = mock_schema_factory(
        columns=["id", "date_utc", "success"],
        pk="id"
    )
    
    with patch.dict(SCHEMA_REGISTRY, {"launches": mock_schema}, clear=False):
        result = transformer.transform("launches", data)
        assert isinstance(result, pl.DataFrame)


# Teste de Cast de String para Boolean (se necessário)

def test_transform_cast_string_to_bool_with_mapping(transformer, mock_schema_factory):
    """Testa cast de string 'true'/'false' para boolean usando mapeamento"""
    
    data = [{
        "id": "123",
        "success": "true"  # String
    }]
    
    
    mock_schema = mock_schema_factory(
        columns=["id", "success"],
        pk="id",
        casts={"success": pl.Boolean}  # Isso vai falhar com string
    )
    
    with patch.dict(SCHEMA_REGISTRY, {"launches": mock_schema}, clear=False):
        
        with pytest.raises(pl.exceptions.InvalidOperationError):
            transformer.transform("launches", data)