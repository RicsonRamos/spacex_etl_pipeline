import logging
import sys
from pathlib import Path

def setup_logger(name, log_file=None):
    """
    Configura um logger que sempre loga no console e opcionalmente em arquivo.
    """
    logger = logging.getLogger(name)
    
    # Evita duplicar handlers se a função for chamada duas vezes
    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Handler para Console (Essencial para Docker/Cloud)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Handler para Arquivo (Opcional)
    if log_file:
        log_path = Path("data/logs")
        log_path.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_path / log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger