from .launches import LaunchExtract
from .rockets import RocketExtract

EXTRACTORS = {
    "launches": LaunchExtract,
    "rockets": RocketExtract,
}

def get_extractor(endpoint: str) -> BaseExtractor:
    """
    Returns an instance of an extractor for the given endpoint.

    Args:
        endpoint (str): The name of the endpoint.

    Returns:
        BaseExtractor: An instance of an extractor.

    Raises:
        ValueError: If the extractor for the given endpoint is not found.
    """
    cls = EXTRACTORS.get(endpoint)
    if not cls:
        raise ValueError(f"Extractor para endpoint '{endpoint}' não encontrado")
    return cls()
