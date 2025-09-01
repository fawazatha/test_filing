from __future__ import annotations

def is_direction_consistent(tx_type: str, before: int, after: int) -> bool:
    tx = (tx_type or "").lower()
    if tx=="buy"  and not (after>before): return False
    if tx=="sell" and not (after<before): return False
    return True

def build_tags(tx_type: str, before_pct: float, after_pct: float):
    tx = (tx_type or "").lower()
    if tx=="sell":
        tags = ["Bearish","Divestment","Ownership Change","Insider Trading"]
    elif tx=="buy":
        tags = ["Bullish","Investment","Ownership Change","Insider Trading"]
    else:
        tags = ["Neutral","Ownership Change","Insider Trading"]
    if (before_pct<50<=after_pct) or (before_pct>=50>after_pct):
        tags.append("Takeover")
    return tags
