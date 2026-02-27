import pytest
from unittest import mock
from src.loaders.schema_validator import SchemaValidator


def test_validate_table_columns_success():
    """
    Testa validação quando todas as colunas existem (cenário feliz).
    """
    validator = SchemaValidator()
    
    # Mocka o engine para não conectar no PostgreSQL real
    with mock.patch.object(validator, 'engine') as mock_engine:
        # Configura mock da conexão como context manager
        mock_conn = mock.Mock()
        mock_engine.connect.return_value.__enter__ = mock.Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = mock.Mock(return_value=False)
        
        # Simula colunas existentes no banco
        mock_conn.execute.return_value = [
            ('id',), ('name',), ('date_utc',)
        ]
        
        # Não deve lançar exceção
        result = validator.validate_table_columns(
            "test_table", 
            ["id", "name", "date_utc"]
        )
        
        # Verifica que o método retorna None (sucesso silencioso)
        assert result is None
        
        # Verifica que a query foi executada corretamente
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        assert "test_table" in str(call_args)


def test_validate_table_columns_missing():
    """
    Testa validação quando há colunas faltantes.
    Deve lançar ValueError com as colunas ausentes.
    """
    validator = SchemaValidator()
    
    with mock.patch.object(validator, 'engine') as mock_engine:
        mock_conn = mock.Mock()
        mock_engine.connect.return_value.__enter__ = mock.Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = mock.Mock(return_value=False)
        
        # Simula apenas 3 colunas existentes (faltando 'age')
        mock_conn.execute.return_value = [
            ('id',), ('name',), ('date_utc',)  # 'age' está faltando!
        ]
        
        # Deve lançar ValueError
        with pytest.raises(ValueError) as exc_info:
            validator.validate_table_columns(
                "test_table", 
                ["id", "name", "date_utc", "age"]  # age não existe no mock
            )
        
        # Verifica mensagem de erro
        assert "age" in str(exc_info.value)
        assert "test_table" in str(exc_info.value)
        assert "Missing columns" in str(exc_info.value)


def test_validate_table_columns_table_not_exists():
    """
    Testa quando tabela não existe no banco.
    Deve retornar silenciosamente (sem erro) após log de warning.
    """
    validator = SchemaValidator()
    
    with mock.patch.object(validator, 'engine') as mock_engine:
        mock_conn = mock.Mock()
        mock_engine.connect.return_value.__enter__ = mock.Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = mock.Mock(return_value=False)
        
        # Tabela vazia = não existe
        mock_conn.execute.return_value = []
        
        # Não deve lançar exceção, apenas log warning
        result = validator.validate_table_columns(
            "nonexistent_table", 
            ["id", "name"]
        )
        
        assert result is None


def test_validate_table_columns_sqlalchemy_error():
    """
    Testa quando há erro de conexão com o banco.
    Deve propagar a exceção SQLAlchemyError.
    """
    from sqlalchemy.exc import OperationalError
    
    validator = SchemaValidator()
    
    with mock.patch.object(validator, 'engine') as mock_engine:
        # Simula erro de conexão
        mock_engine.connect.side_effect = OperationalError(
            "connection failed", 
            params=None, 
            orig=Exception("DB down")
        )
        
        # Deve propagar a exceção
        with pytest.raises(OperationalError):
            validator.validate_table_columns("test_table", ["id"])


def test_validator_initialization():
    """
    Testa que o SchemaValidator pode ser instanciado corretamente.
    """
    validator = SchemaValidator()
    
    assert validator is not None
    assert hasattr(validator, 'engine')
    assert hasattr(validator, 'validate_table_columns')