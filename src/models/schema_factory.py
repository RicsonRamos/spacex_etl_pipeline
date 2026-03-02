from src.models.schemas import LaunchesSchema
from src.utils.logger import get_logger

logger = get_logger(__name__)

class SchemaFactory:
    """
    Centraliza a lógica de qual schema aplicar a cada dataset
    Rigor: Evita acoplamento direto entre Main e as Clases de validação

    """
    _SCHEMAS = {
        "launches": LaunchesSchema
    }
    
    
    
    @classmethod
    def get_validator(cls, schema_name: str):
        validator = cls._SCHEMAS.get(schema_name)
        if not validator:
            logger.error(f"Schema '{schema_name}' não está registrado no Factory.")
            raise ValueError(f"Schema Inválido: {schema_name}")
        return validator