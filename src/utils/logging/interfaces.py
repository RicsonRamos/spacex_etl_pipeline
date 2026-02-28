from abc import ABC, abstractmethod


class LoggerConfigurator(ABC):
    @abstractmethod
    def configure(self) -> None:
        pass