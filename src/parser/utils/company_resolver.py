from __future__ import annotations

import os
import re
import json
import unicodedata
import logging
from pathlib import Path
from typing import Dict, Optional, Tuple, List
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

DEFAULT_MAP_PATH = Path(os.getenv("COMPANY_MAP_FILE", "data/company/company_map.json"))

# Tokens to drop when building normalized keys for matching
_CORP_STOPWORDS = {
    "PT", "P.T", "PERSEROAN", "TERBATAS",
    "TBK", "TBK.", "TBK,", "TBK)", "(TBK",
    "PERSERO", "(PERSERO)"
}

_TOKEN_SPLIT = re.compile(r"[^A-Z0-9]+", re.UNICODE)
_SPLIT_RE = re.compile(r"[^A-Za-z0-9]+", re.UNICODE)

# Tokens to keep uppercased when formatting display names
_COMMON_UPPER = {
    "PT", "CV", "UD", "LLC", "LLP", "INC", "NV", "BV", "GMBH", "BHD", "PLC", "RI",
    "OJK", "KPK", "BPK", "BPKP"
}


def _strip_diacritics(s: str) -> str:
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")


def normalize_company_name(s: str) -> str:
    """
    Build a robust, normalized key for matching:
    - remove diacritics
    - uppercase
    - '&' → ' AND '
    - drop corporate stopwords
    - collapse spaces and non-alnum
    """
    s = _strip_diacritics(s).upper().replace("&", " AND ")
    tokens = [t for t in _TOKEN_SPLIT.split(s) if t]
    tokens = [t for t in tokens if t not in _CORP_STOPWORDS]
    return " ".join(tokens).strip()


def _normalize_name(s: str) -> str:
    """Lowercase key used by _load_local_company_map() for quick exact lookups."""
    if not s:
        return ""
    s = s.strip()
    tokens = [t for t in _SPLIT_RE.split(s.lower()) if t]
    tokens = [t for t in tokens if t.upper() not in _CORP_STOPWORDS]
    return " ".join(tokens).strip()


def _load_local_company_map(self):
    """Read data/company/company_map.json (symbol -> company_name)."""
    try:
        if DEFAULT_MAP_PATH.exists():
            data = json.loads(DEFAULT_MAP_PATH.read_text(encoding="utf-8"))
            self._symbol_to_name = {(k or "").upper(): (v or "") for k, v in data.items() if k and v}
            self._name_to_symbol = {}
            for sym, raw_name in self._symbol_to_name.items():
                key = _normalize_name(raw_name)
                if key and key not in self._name_to_symbol:
                    self._name_to_symbol[key] = sym
        else:
            self._symbol_to_name, self._name_to_symbol = {}, {}
    except Exception as e:
        logger.warning(f"Failed to load local company map: {e}")
        self._symbol_to_name, self._name_to_symbol = {}, {}


def load_symbol_to_name_from_file(path: Path = DEFAULT_MAP_PATH) -> Optional[Dict[str, str]]:
    """
    Load company map from JSON. Accepts either:
      { "ABCD": "PT Alpha Beta Tbk", ... }
      or      { "ABCD": {"company_name": "...", ...}, ... }
    Adds both BASE and BASE.JK aliases.
    """
    try:
        if not path.exists():
            logger.warning(f"Company map not found: {path}")
            return None

        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            logger.error("company_map must be a dict: symbol -> {company_name,...} or string")
            return None

        out: Dict[str, str] = {}

        def _add(sym: str, nm: str) -> None:
            s = (sym or "").strip().upper()
            n = (nm or "").strip()
            if not s or not n:
                return
            if s.endswith(".JK"):
                base = s[:-3]
                out[base] = n
                out[s] = n
            else:
                out[s] = n
                out[f"{s}.JK"] = n

        for sym, val in raw.items():
            if isinstance(val, dict):
                name = val.get("company_name") or val.get("name") or val.get("legal_name") or ""
            else:
                name = str(val or "")
            _add(sym, name)

        logger.info(f"Loaded {len(out)} symbol entries (with .JK aliases) from {path}")
        return out

    except Exception as e:
        logger.warning(f"Failed reading company_map file {path}: {e}")
        return None


def build_reverse_map(symbol_to_name: Dict[str, str]) -> Dict[str, List[str]]:
    """
    Build reverse map: normalized company name -> [symbols].
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


def canonical_name_for_symbol(symbol_to_name: Dict[str, str], symbol: str) -> Optional[str]:
    """Return canonical company name for a given symbol (handles BASE and BASE.JK)."""
    s = (symbol or "").strip().upper()
    if not s:
        return None
    if s in symbol_to_name:
        return symbol_to_name[s]
    if s.endswith(".JK"):
        return symbol_to_name.get(s[:-3])
    return symbol_to_name.get(f"{s}.JK")


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

def pretty_company_name(raw: str) -> str:
    """
    Human-friendly fallback formatter for company names:
    - Standardize 'PT' variants and 'Tbk'
    - Keep common acronyms uppercase (PT, LLC, INC, etc.)
    - Title-case the rest (hyphen-aware)
    - Preserve spacing and punctuation (& , ( ) . -)
    """
    if not raw:
        return ""
    s = _strip_diacritics(raw).strip()

    # Standardize PT variants and TBK spelling
    s = re.sub(r"\bP\.?\s*T\.?\b", "PT", s, flags=re.I)
    s = re.sub(r"\bTBK\.?\b", "Tbk", s, flags=re.I)

    # Split but keep separators
    parts = re.split(r"(\s+|[&(),.-])", s)

    def fmt(tok: str) -> str:
        if not tok or tok.isspace() or tok in "&(),.-":
            return tok
        up = tok.upper()
        if up in _COMMON_UPPER:
            return up
        if up == "TBK":
            return "Tbk"
        # hyphenated: "multi-purpose" -> "Multi-Purpose"
        if "-" in tok:
            return "-".join(p[:1].upper() + p[1:].lower() if p else p for p in tok.split("-"))
        return tok[:1].upper() + tok[1:].lower()

    out = "".join(fmt(t) for t in parts)
    out = re.sub(r"\s+", " ", out).strip(" ,.;-")
    out = out.replace(" ,", ",").replace(" .", ".")
    return out

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
