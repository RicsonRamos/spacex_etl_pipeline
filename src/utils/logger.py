import logging
import sys
from pathlib import Path

def setup_logger(name, log_file=None):
    """
    Configures a logger that always logs to console and optionally to a file.

    :param name: The name of the logger.
    :param log_file: The name of the log file to write to. If None, no file will be written.
    :return: The configured logger.
    """
    logger = logging.getLogger(name)

    # Avoids duplicating handlers if the function is called twice
    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.INFO)

    # Define the log format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Handler for Console (Essential for Docker/Cloud)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Handler for File (Optional)
    if log_file:
        log_path = Path("data/logs")
        log_path.mkdir(parents=True, exist_ok=True)

        # Reuse existing file handler if it already exists
        existing_handler = next(
            (handler for handler in logger.handlers
            if isinstance(handler, logging.FileHandler) and handler.baseFilename == str(log_path / log_file)),
            None
        )

        if existing_handler:
            file_handler = existing_handler
        else:
            file_handler = logging.FileHandler(log_path / log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    return logger