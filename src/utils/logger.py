import logging
import sys
import json

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "time": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "module": record.module,
            "message": record.getMessage(),
            "name": record.name,
        }
        return json.dumps(log_record)

def get_logger(name: str, json_logs: bool = False):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        if json_logs:
            formatter = JsonFormatter()
        else:
            # Formato legível para console
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(module)s - %(message)s',
            )

        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger