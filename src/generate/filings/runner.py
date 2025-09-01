from .utils.pipeline import generate_filings

def run(**kwargs) -> int:
    return generate_filings(**kwargs)
