from .utils.pipeline import run as generate_filings


def run(**kwargs) -> int:
    return generate_filings(**kwargs)
