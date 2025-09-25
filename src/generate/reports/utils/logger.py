import logging
import os

def get_logger(name: str = "report", level: str | None = None) -> logging.Logger:
    lvl = (level or os.getenv("LOGLEVEL", "INFO")).upper()
    logger = logging.getLogger(name)
    if logger.handlers:
        logger.setLevel(lvl)
        return logger

    handler = logging.StreamHandler()
    fmt = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(handler)
    logger.setLevel(lvl)
    return logger
