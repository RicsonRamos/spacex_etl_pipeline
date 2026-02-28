from abc import ABC, abstractmethod


class ExtractorPort(ABC):
    @abstractmethod
    def extract(self, real_api: bool):
        pass


class TransformerPort(ABC):
    @abstractmethod
    def transform(self, raw_data, last_ingested):
        pass


class BronzeLoaderPort(ABC):
    @abstractmethod
    def load(self, raw_data, entity: str, source: str):
        pass


class SilverLoaderPort(ABC):
    @abstractmethod
    def upsert(self, df, entity: str) -> int:
        pass


class WatermarkPort(ABC):
    @abstractmethod
    def get_last_ingested(self, entity: str):
        pass


class MetricsPort(ABC):
    @abstractmethod
    def inc_extract(self, entity: str, value: int):
        pass

    @abstractmethod
    def inc_silver(self, entity: str, value: int):
        pass


class NotifierPort(ABC):
    @abstractmethod
    def notify(self, message: str):
        pass