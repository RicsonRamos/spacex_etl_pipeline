# src/utils/monitoring/metrics.py

from prometheus_client import Counter, Histogram

class PipelineMetrics:
    _instance = None  # singleton

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            
            # Inicializa métricas apenas uma vez
            cls._instance.extract_count = Counter(
                "spacex_extract_count", "Registros extraídos", ["entity"]
            )
            cls._instance.silver_count = Counter(
                "spacex_silver_count", "Registros na Silver", ["entity"]
            )
            cls._instance.flow_time = Histogram(
                "spacex_flow_duration_seconds", "Tempo de execução do flow", ["flow_name"]
            )
        return cls._instance

    def inc_extract(self, entity: str, value: int = 1):
        self.extract_count.labels(entity).inc(value)

    def inc_silver(self, entity: str, value: int = 1):
        self.silver_count.labels(entity).inc(value)



# Novas classes necessárias


class Watermark:
    """
    Guarda o último valor processado por entidade.
    """
    def __init__(self):
        self._last_processed = {}

    def update(self, entity: str, value):
        self._last_processed[entity] = value

    def get(self, entity: str):
        return self._last_processed.get(entity, None)


class SchemaValidator:
    """
    Valida o schema de dados por entidade.
    """
    def validate(self, entity: str, data):
        # Aqui você implementa a lógica de validação
        # Por enquanto, retorna True como placeholder
        return True