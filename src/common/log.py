import logging

"""Logger setup to enforce consistent formatting."""

def get_logger(name: str = "app", level: int = logging.INFO) -> logging.Logger:
    """Return a console logger with a simple, consistent format."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("[%(levelname)s] %(asctime)s %(name)s: %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger
