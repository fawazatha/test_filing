from __future__ import annotations
from .normalizers import safe_int, safe_float

def determine_transaction_type_from_list(txs):
    buys  = [t for t in txs if t.get("type")=="buy"]
    sells = [t for t in txs if t.get("type")=="sell"]
    buy_val  = sum(safe_float(t.get("price"))*safe_int(t.get("amount")) for t in buys)
    sell_val = sum(safe_float(t.get("price"))*safe_int(t.get("amount")) for t in sells)
    return ("sell", sell_val-buy_val, sells) if sell_val>buy_val else ("buy", buy_val-sell_val, buys)

def average_price(txs):
    amt = sum(safe_int(t.get("amount")) for t in txs)
    if amt<=0: return 0.0
    val = sum(safe_float(t.get("price"))*safe_int(t.get("amount")) for t in txs)
    return round(val/amt, 2)
