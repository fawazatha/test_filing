from __future__ import annotations
from typing import List

def weighted_average_price(prices: List[float], amounts: List[int]) -> float:
    if not prices or not amounts or len(prices) != len(amounts):
        return 0.0
    num = 0.0
    den = 0
    for p, a in zip(prices, amounts):
        try:
            num += float(p) * int(a)
            den += int(a)
        except Exception:
            pass
    return float(num / den) if den > 0 else 0.0

def transaction_value(prices: List[float], amounts: List[int]) -> float:
    w = weighted_average_price(prices, amounts)
    total_amt = sum(int(a) for a in amounts if a is not None)
    return float(w * total_amt) if total_amt > 0 else 0.0
