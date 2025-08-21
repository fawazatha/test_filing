import logging
import sys

def get_logger(name: str = "downloader", verbose: bool = False) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
