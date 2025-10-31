from __future__ import annotations
from typing import Dict, Optional, Tuple, List
from difflib import SequenceMatcher

from src.common.log import get_logger
from .format import normalize_company_name, pretty_company_name, canonical_name_for_symbol

"""
This module contains only the business logic for resolving, suggesting,
and transforming company data structures (no I/O or formatting).
"""

logger = get_logger(__name__)

# Data Structure Transformers
def build_reverse_map(symbol_to_name: Dict[str, str]) -> Dict[str, List[str]]:
    """
    Build reverse map: normalized company key -> [symbols].
    Uses UPPER-key normalization for robustness.
    """
    rev: Dict[str, List[str]] = {}
    for sym, raw_name in (symbol_to_name or {}).items():
        key = normalize_company_name(raw_name)
        if not key:
            continue
        bucket = rev.setdefault(key, [])
        if sym not in bucket:
            bucket.append(sym)
    return rev


# Resolution & Suggestion Logic
def resolve_symbol_from_emiten(
    emiten_raw: str,
    symbol_to_name: Dict[str, str],
    rev_map: Optional[Dict[str, List[str]]] = None,
    fuzzy: bool = True,
    min_score: int = 85
) -> Tuple[Optional[str], str, List[str]]:
    """
    Try to resolve a symbol from a raw company/emiten string.
    Returns (symbol|None, normalized_query_key, tried_list).
    """
    tried: List[str] = []
    if not symbol_to_name:
        return None, "", tried

    if rev_map is None:
        rev_map = build_reverse_map(symbol_to_name)

    q = normalize_company_name(emiten_raw)
    tried.append(q)

    # Exact normalized-key match
    syms = rev_map.get(q)
    if syms:
        for s in syms:
            if normalize_company_name(symbol_to_name.get(s, "")) == q:
                return s, q, tried
        return syms[0], q, tried  # fallback first

    # Fuzzy key match
    if fuzzy and rev_map:
        best_key = None
        best_score = -1.0
        for k in rev_map.keys():
            score = SequenceMatcher(None, q, k).ratio() * 100.0
            if score > best_score:
                best_key, best_score = k, score
        if best_key:
            tried.append(f"fuzzy:{best_key}:{int(best_score)}")
            if best_score >= float(min_score):
                syms2 = rev_map.get(best_key, [])
                if syms2:
                    for s in syms2:
                        if normalize_company_name(symbol_to_name.get(s, "")) == best_key:
                            return s, best_key, tried
                    return syms2[0], best_key, tried

    return None, q, tried


def suggest_symbols(
    emiten_raw: str,
    symbol_to_name: Dict[str, str],
    rev_map: Optional[Dict[str, List[str]]] = None,
    top_k: int = 3
) -> List[Dict[str, str]]:
    """
    Suggest top-K symbols based on normalized-key similarity.
    Returns list of {symbol, company_name, score, normalized_key}.
    """
    if not emiten_raw or not symbol_to_name:
        return []
    if rev_map is None:
        rev_map = build_reverse_map(symbol_to_name)

    def _base(sym: str) -> str:
        s = (sym or "").strip().upper()
        return s[:-3] if s.endswith(".JK") else s

    q = normalize_company_name(emiten_raw)

    scored: List[Tuple[str, float]] = []
    for key in rev_map.keys():
        score = SequenceMatcher(None, q, key).ratio() * 100.0
        scored.append((key, score))
    scored.sort(key=lambda x: x[1], reverse=True)

    out: List[Dict[str, str]] = []
    seen_bases = set()

    for key, score in scored:
        syms = sorted(rev_map.get(key, []), key=lambda s: (0 if s.upper().endswith(".JK") else 1, s))
        for sym in syms:
            base = _base(sym)
            if base in seen_bases:
                continue
            out.append({
                "symbol": sym,
                "company_name": symbol_to_name.get(sym, ""),
                "score": str(int(round(score))),
                "normalized_key": key,
            })
            seen_bases.add(base)
            if len(out) >= max(int(top_k or 0), 0):
                return out

    return out


# Composite Facade Function 
def resolve_symbol_and_name(
    emiten_raw: str,
    symbol_to_name: Dict[str, str],
    rev_map: Optional[Dict[str, List[str]]] = None,
    fuzzy: bool = True,
    min_score: int = 85
) -> Tuple[Optional[str], str, str, List[str]]:
    """
    Try to resolve symbol; if found, return canonical mapped name.
    If not found, return pretty formatted fallback name.

    Returns:
      (symbol|None, display_name, matched_key, tried_list)
    """
    sym, key, tried = resolve_symbol_from_emiten(
        emiten_raw, symbol_to_name, rev_map=rev_map, fuzzy=fuzzy, min_score=min_score
    )
    if sym:
        disp = canonical_name_for_symbol(symbol_to_name, sym) or pretty_company_name(emiten_raw)
        return sym, disp, key, tried
    return None, pretty_company_name(emiten_raw), key, tried