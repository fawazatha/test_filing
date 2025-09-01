from __future__ import annotations
from typing import Any, Dict, List, Tuple
from .formats import dump_model, get_attr

def extract_transactions(obj: Any) -> List[Dict]:
    tx = get_attr(obj, "transactions", []) or []
    return [dump_model(t) if not isinstance(t, dict) else t for t in tx]

def choose_direction_and_legs(txns: List[Dict]) -> Tuple[str, List[Dict]]:
    buys = [t for t in txns if (t.get("type") or "").lower() == "buy"]
    sells = [t for t in txns if (t.get("type") or "").lower() == "sell"]
    vb = sum(float(t.get("value") or 0.0) for t in buys)
    vs = sum(float(t.get("value") or 0.0) for t in sells)
    if vb > vs and vb > 0: return "buy", buys
    if vs > vb and vs > 0: return "sell", sells
    if buys and not sells: return "buy", buys
    if sells and not buys: return "sell", sells
    return "unknown", txns

def weighted_avg(prices, amounts) -> float:
    if not prices or not amounts or len(prices) != len(amounts):
        return 0.0
    total = sum(int(a or 0) for a in amounts)
    if total <= 0: return 0.0
    return sum(float(p or 0.0) * int(a or 0) for p, a in zip(prices, amounts)) / total

def inconsistent(direction: str, holding_before: int, holding_after: int) -> bool:
    if direction == "buy"  and holding_after < holding_before: return True
    if direction == "sell" and holding_after > holding_before: return True
    return False
