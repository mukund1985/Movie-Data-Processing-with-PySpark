import logging
import os
from datetime import datetime

LOG_DIR = os.getenv("LOG_DIR", "logs/")
VERSIONING = datetime.now().strftime("%Y%m%d%H%M%S")

def get_logger(name, with_file_handler=True):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    if with_file_handler:
        file_handler = logging.FileHandler(os.path.join(LOG_DIR, f"{name}_{VERSIONING}.log"))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
