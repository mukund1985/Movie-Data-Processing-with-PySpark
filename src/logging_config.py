import logging
import os
from datetime import datetime

# Default directory for logging
LOG_DIR = os.getenv("LOG_DIR", "logs/")
# Ensure the log directory exists
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

def get_logger(name, log_file=None):
    """
    Creates and returns a logger with specified configurations.
    
    :param name: Name of the logger, typically the module's name.
    :param log_file: Optional. Custom path for the log file. If not provided, defaults to a standard naming convention in LOG_DIR.
    :return: Configured logger object.
    """
    # Initialize logger with the given name
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # If no specific log_file path is provided, use a default naming scheme
    if not log_file:
        versioning = datetime.now().strftime("%Y%m%d%H%M%S")
        log_file = os.path.join(LOG_DIR, f"{name}_{versioning}.log")

    # File handler for writing logs to a file
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler for printing logs to the console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Prevent logging from propagating to the root logger
    logger.propagate = False

    return logger
