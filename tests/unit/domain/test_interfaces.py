# tests/unit/domain/test_interfaces.py
"""
Testes para interfaces do domínio (Ports).
Verifica contratos abstratos que definem a arquitetura hexagonal.
"""

import pytest
from abc import ABC
from src.domain.Interfaces import (
    ExtractorPort,
    TransformerPort,
    BronzeLoaderPort,
    SilverLoaderPort,
    WatermarkPort,
    MetricsPort,
    NotifierPort,
)


class TestExtractorPort:
    """Testa contrato de extração de dados."""
    
    def test_is_abstract_class(self):
        """Deve ser classe abstrata."""
        assert issubclass(ExtractorPort, ABC)
    
    def test_cannot_instantiate_directly(self):
        """Não deve ser instanciável sem implementação."""
        with pytest.raises(TypeError, match="abstract"):
            ExtractorPort()
    
    def test_extract_method_is_abstract(self):
        """Método extract deve ser abstrato."""
        assert getattr(ExtractorPort.extract, '__isabstractmethod__', False)
    
    def test_valid_implementation(self):
        """Implementação válida deve funcionar."""
        class MockExtractor(ExtractorPort):
            def extract(self, real_api: bool):
                return [{"id": 1}] if real_api else []
        
        extractor = MockExtractor()
        assert extractor.extract(real_api=True) == [{"id": 1}]
        assert extractor.extract(real_api=False) == []


class TestTransformerPort:
    """Testa contrato de transformação de dados."""
    
    def test_is_abstract_class(self):
        assert issubclass(TransformerPort, ABC)
    
    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError):
            TransformerPort()
    
    def test_valid_implementation(self):
        """Implementação deve receber raw_data e last_ingested."""
        class MockTransformer(TransformerPort):
            def transform(self, raw_data, last_ingested):
                return {"data": raw_data, "watermark": last_ingested}
        
        transformer = MockTransformer()
        result = transformer.transform([{"id": 1}], "2024-01-01")
        
        assert result["data"] == [{"id": 1}]
        assert result["watermark"] == "2024-01-01"


class TestBronzeLoaderPort:
    """Testa contrato de carga na camada Bronze."""
    
    def test_is_abstract_class(self):
        assert issubclass(BronzeLoaderPort, ABC)
    
    def test_load_signature(self):
        """Método load deve aceitar raw_data, entity e source."""
        class MockBronzeLoader(BronzeLoaderPort):
            def load(self, raw_data, entity: str, source: str):
                return f"loaded_{entity}"
        
        loader = MockBronzeLoader()
        result = loader.load([{"id": 1}], "rockets", "api")
        
        assert result == "loaded_rockets"


class TestSilverLoaderPort:
    """Testa contrato de carga na camada Silver."""
    
    def test_is_abstract_class(self):
        assert issubclass(SilverLoaderPort, ABC)
    
    def test_upsert_returns_int(self):
        """Método upsert deve retornar número de registros."""
        class MockSilverLoader(SilverLoaderPort):
            def upsert(self, df, entity: str) -> int:
                return 10
        
        loader = MockSilverLoader()
        assert loader.upsert(None, "rockets") == 10


class TestWatermarkPort:
    """Testa contrato de gerenciamento de watermark."""
    
    def test_get_last_ingested_signature(self):
        """Deve retornar última data de ingestão."""
        class MockWatermark(WatermarkPort):
            def get_last_ingested(self, entity: str):
                from datetime import datetime
                return datetime(2024, 1, 1)
        
        wm = MockWatermark()
        result = wm.get_last_ingested("rockets")
        
        assert result.year == 2024


class TestMetricsPort:
    """Testa contrato de métricas."""
    
    def test_has_two_abstract_methods(self):
        """Deve definir inc_extract e inc_silver."""
        assert hasattr(MetricsPort, 'inc_extract')
        assert hasattr(MetricsPort, 'inc_silver')
    
    def test_valid_implementation(self):
        """Implementação deve contar métricas."""
        class MockMetrics(MetricsPort):
            def __init__(self):
                self.counts = {}
            
            def inc_extract(self, entity: str, value: int):
                self.counts[f"extract_{entity}"] = value
            
            def inc_silver(self, entity: str, value: int):
                self.counts[f"silver_{entity}"] = value
        
        metrics = MockMetrics()
        metrics.inc_extract("rockets", 5)
        metrics.inc_silver("rockets", 3)
        
        assert metrics.counts["extract_rockets"] == 5
        assert metrics.counts["silver_rockets"] == 3


class TestNotifierPort:
    """Testa contrato de notificação."""
    
    def test_notify_signature(self):
        """Deve aceitar mensagem string."""
        class MockNotifier(NotifierPort):
            def __init__(self):
                self.messages = []
            
            def notify(self, message: str):
                self.messages.append(message)
        
        notifier = MockNotifier()
        notifier.notify("Pipeline failed")
        
        assert "Pipeline failed" in notifier.messages