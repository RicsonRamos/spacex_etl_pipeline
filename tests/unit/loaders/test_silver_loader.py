# tests/unit/loaders/test_silver_loader.py (atualizado)
"""
Testes para SilverLoader.
Verifica upsert na camada Silver.
"""

import pytest
from unittest import mock
import polars as pl

from src.loaders.silver_loader import SilverLoader


class TestSilverLoader:
    """Testes para loader Silver."""
    
    @pytest.fixture
    def loader(self):
        """Loader com engine mockado."""
        with mock.patch('src.loaders.silver_loader.DatabaseConnection.__init__', return_value=None):
            loader = SilverLoader(table_name="test_table")
            loader.engine = mock.Mock()
            return loader
    
    def test_upsert_requires_table_name(self, loader):
        """Deve exigir nome da tabela."""
        loader.table_name = None
        
        df = pl.DataFrame({"id": [1], "name": ["test"]})
        
        with pytest.raises(ValueError, match="Table name must be provided"):
            loader.upsert(df)
    
    def test_upsert_uses_entity_parameter(self, loader):
        """Deve usar entity como nome da tabela quando informado."""
        df = pl.DataFrame({
            "id": [1],
            "rocket": ["falcon9"],
            "date_utc": ["2024-01-01"],
            "success": [True],
            "details": ["test"],
        })
        
        with mock.patch.object(loader.engine, 'begin') as mock_begin:
            mock_conn = mock.Mock()
            mock_begin.return_value.__enter__ = mock.Mock(return_value=mock_conn)
            mock_begin.return_value.__exit__ = mock.Mock(return_value=False)
            
            result = loader.upsert(df, entity="launches")
            
            assert result == 1
            # Verifica que query foi executada
            mock_conn.execute.assert_called_once()