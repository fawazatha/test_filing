import logging
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

JKT = ZoneInfo("Asia/Jakarta")


class JakartaFormatter(logging.Formatter):
    """Force timestamps to Asia/Jakarta in log output."""
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=JKT)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat(sep=" ", timespec="seconds")


def get_logger(name: str, verbose: bool = False) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    if not logger.handlers:
        h = logging.StreamHandler(stream=sys.stdout)
        fmt = JakartaFormatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
        h.setFormatter(fmt)
        logger.addHandler(h)
        logger.propagate = False
    return logger
