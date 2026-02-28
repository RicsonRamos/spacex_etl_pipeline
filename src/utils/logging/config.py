from dataclasses import dataclass


@dataclass(frozen=True)
class LoggingConfig:
    log_level: str
    json_output: bool