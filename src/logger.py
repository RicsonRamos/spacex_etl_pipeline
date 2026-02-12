import logging
from pathlib import Path


def setup_logger(name: str, log_file: str):
    """
    Setup a logger with a file handler and a stream handler.

    :param name: The name of the logger.
    :param log_file: The name of the log file.
    :return: A logger object.
    """

    # Create the log directory if it doesn't exist
    log_dir = Path('data/logs')
    log_dir.mkdir(parents=True, exist_ok=True)

    # Create a logger object
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Create a formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    # Create a file handler and a stream handler
    file_handler = logging.FileHandler(str(log_dir / log_file), mode='a')
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.handlers.clear()  # Clear existing handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
