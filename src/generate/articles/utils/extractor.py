from __future__ import annotations
import re
from typing import Dict, Any, List, Optional
from .io_utils import get_logger

log = get_logger(__name__)

SYMBOL_RE = re.compile(r"\b([A-Z0-9]{3,6})(?:\.JK)?\b")
BUY_WORDS = ("buy", "pembelian", "membeli", "acquire", "pembelian saham")
SELL_WORDS = ("sell", "penjualan", "menjual", "dispose", "penjualan saham")
TRANSFER_WORDS = ("transfer", "pengalihan", "alih", "pindah")

def _maybe_symbol(text: str) -> Optional[str]:
    m = re.search(r"\b([A-Z0-9]{3,6})\.JK\b", text.upper())
    if m:
        return m.group(1) + ".JK"
    m2 = SYMBOL_RE.search(text.upper())
    if m2:
        return m2.group(1) + ".JK"
    return None

def _infer_tx_type(text: str) -> str:
    t = text.lower()
    if any(w in t for w in TRANSFER_WORDS):
        return "transfer"
    if any(w in t for w in BUY_WORDS):
        return "buy"
    if any(w in t for w in SELL_WORDS):
        return "sell"
    return ""

NUM_RE = re.compile(r"(?<![\w\.])([0-9]{1,3}(?:[.,][0-9]{3})*|[0-9]+)(?![\w\.])")

def _extract_amounts_block(text: str) -> List[int]:
    amts: List[int] = []
    for m in NUM_RE.finditer(text):
        s = m.group(1).replace(".", "").replace(",", "")
        try:
            v = int(s)
            if v >= 100:
                amts.append(v)
        except Exception:
            pass
    return amts[:5]

PRICE_RE = re.compile(r"(?:Rp|IDR)?\s*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)")

def _extract_prices_block(text: str) -> List[float]:
    prices: List[float] = []
    for m in PRICE_RE.finditer(text):
        s = m.group(1).replace(".", "").replace(",", ".")
        try:
            v = float(s)
            if v > 0:
                prices.append(v)
        except Exception:
            pass
    return prices[:5]

def extract_info_from_text(text: str) -> Dict[str, Any]:
    symbol = _maybe_symbol(text) or None
    tx_type = _infer_tx_type(text)
    info = {
        "symbol": symbol,
        "holder_type": "",
        "holder_name": "",
        "transaction_type": tx_type,
        "prices": _extract_prices_block(text),
        "amount_transacted": _extract_amounts_block(text),
    }
    log.debug(f"extract_info_from_text -> {info}")
    return info
