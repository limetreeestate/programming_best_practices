import logging
from datetime import datetime as dt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Example")


def log_info(message: str) -> None:
    logger.info(f"{dt.now()}: {message}")


def log_error(message: str) -> None:
    logger.error(f"{dt.now()}: {message}")




