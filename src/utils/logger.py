import logging, os
from pathlib import Path

_LOGGERS = {}
_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

def get_logger(name="idx"):
    if name in _LOGGERS: return _LOGGERS[name]
    level = os.getenv("LOG_LEVEL","INFO").upper()
    logging.basicConfig(level=level, format=_FMT)
    lg = logging.getLogger(name)
    _LOGGERS[name] = lg
    return lg

def dump_debug_text(basename: str, text: str, out_dir="debug_output"):
    if os.getenv("DEBUG_TEXT","1") != "1": return
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    (Path(out_dir)/f"{basename}.txt").write_text(text or "", encoding="utf-8")
