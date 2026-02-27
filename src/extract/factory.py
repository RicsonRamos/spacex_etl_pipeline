from src.extract.launches import LaunchExtract
from src.extract.rockets import RocketExtract
from src.extract.base import BaseExtractor

EXTRACTORS = {
    "launches": LaunchExtract,
    "rockets": RocketExtract,
}

def get_extractor(endpoint: str) -> type[BaseExtractor]:
    """
    Retorna a classe do extractor para o endpoint informado.

    Args:
        endpoint (str): O nome do endpoint.

    Returns:
        type[BaseExtractor]: Classe do extractor.

    Raises:
        ValueError: Se o extractor não for encontrado.
    """
    cls = EXTRACTORS.get(endpoint)
    if not cls:
        raise ValueError(f"Extractor para endpoint '{endpoint}' não encontrado")
    return cls  # retorna a classe, não a instância