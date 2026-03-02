from abc import ABC, abstractmethod
import pandas as pd

class DataExtractor(ABC):
    """
    Método abstrato para extrair dados de uma fonte.
    
    Este método deve ser sobrescrito por qualquer implementação concreta de extrator de dados.
    Ele deve retornar um DataFrame do pandas contendo os
    dados extraídos.

    Retorna:

    pd.DataFrame: Um DataFrame do pandas contendo os dados extraídos.

    """
    @abstractmethod
    def extract(self) -> pd.DataFrame:

        pass