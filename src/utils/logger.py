import logging
import sys

def get_logger(name: str):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # Formato do log Data | Level | Modulo | Message
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(module)s - %(message)s',
        )

        # Console
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        return logger