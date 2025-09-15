from __future__ import annotations
from typing import List, Dict, Any, Optional
from .generator import ArticleGenerator
from .utils.io_utils import get_logger

log = get_logger(__name__)

def run_from_filings(
    filings: List[Dict[str, Any]],
    company_map_path: str = "data/company/company_map.json",
    latest_prices_path: str = "data/company/latest_prices.json",
    use_llm: bool = False,
    model_name: str = "llama-3.3-70b-versatile",  # override via CLI/env kalau mau 8B
    prefer_symbol: bool = True,
    provider: Optional[str] = None,
) -> List[Dict[str, Any]]:
    gen = ArticleGenerator(
        company_map_path=company_map_path,
        latest_prices_path=latest_prices_path,
        use_llm=use_llm,
        groq_model=model_name,
        prefer_symbol=prefer_symbol,
        provider=provider,
    )
    out: List[Dict[str, Any]] = []
    for f in filings:
        try:
            art = gen.from_filing(f)
            if art:
                out.append(art)
        except Exception as e:
            log.exception(f"Error generating article from filing: {e}")
    return out

def run_from_text_items(
    items: List[Dict[str, Any]],
    company_map_path: str = "data/company/company_map.json",
    latest_prices_path: str = "data/company/latest_prices.json",
    use_llm: bool = False,
    model_name: str = "llama-3.3-70b-versatile",
    prefer_symbol: bool = True,
    provider: Optional[str] = None,
) -> List[Dict[str, Any]]:
    gen = ArticleGenerator(
        company_map_path=company_map_path,
        latest_prices_path=latest_prices_path,
        use_llm=use_llm,
        groq_model=model_name,
        prefer_symbol=prefer_symbol,
        provider=provider,
    )
    out: List[Dict[str, Any]] = []
    for it in items:
        try:
            art = gen.from_text_item(it)
            if art:
                out.append(art)
        except Exception as e:
            log.exception(f"Error generating article from text item: {e}")
    return out
