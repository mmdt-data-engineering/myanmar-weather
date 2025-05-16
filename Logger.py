import logging
from datetime import date
import os


class Logger:
    def __init__(self):
        pass

    def get_logger(self):
        # Setup logging
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        today = date.today()
        str_today = today.strftime("%Y-%m-%d")  # Output like '2025-05-16'
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)

        # Set up logging with dynamic filename
        log_file = os.path.join(log_dir, f"{str_today}.log")

        handler = logging.FileHandler(log_file, mode="a")
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
