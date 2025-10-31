# src/generate/filings/runner.py
from .utils.pipeline import run as generate_filings

def run(**kwargs) -> int:
    """Entrypoint for the generation pipeline."""
    return generate_filings(**kwargs)