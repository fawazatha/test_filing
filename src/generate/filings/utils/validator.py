from __future__ import annotations

from typing import Any, List


def _get(obj: Any, name: str):
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)


class FilingValidation:
    def validate(self, f: Any) -> List[str]:
        warn: List[str] = []
        symbol = _get(f, "symbol")
        holder = _get(f, "holder_name")
        txns = _get(f, "transactions") or []
        h_before = int((_get(f, "holding_before") or 0) or 0)
        h_after = int((_get(f, "holding_after") or 0) or 0)
        p_before = float((_get(f, "share_percentage_before") or 0.0) or 0.0)
        p_after = float((_get(f, "share_percentage_after") or 0.0) or 0.0)

        if not symbol:
            warn.append("missing symbol")
        if not holder:
            warn.append("missing holder_name")

        # NON-IDX snapshot case: no legs is acceptable if there is a delta
        has_snapshot_delta = (h_before != h_after) or (abs(p_after - p_before) > 1e-9)
        if not txns and not has_snapshot_delta:
            warn.append("no transactions present and no snapshot delta")

        # Legs consistency vs holdings delta
        if txns:
            buy_amt = sum(int(t.get("amount") or 0) for t in txns if t.get("type") == "buy")
            sell_amt = sum(int(t.get("amount") or 0) for t in txns if t.get("type") == "sell")
            net_amt = buy_amt - sell_amt
            delta_h = h_after - h_before
            only_transfer = all((t.get("type") == "transfer") for t in txns if t.get("type"))
            if not only_transfer and (net_amt - delta_h) != 0:
                warn.append(f"net amount ({net_amt}) != delta holding ({delta_h})")

        # Price sanity
        price = _get(f, "price")
        if price is not None:
            try:
                p = float(price)
                if p < 0:
                    warn.append("negative price")
            except Exception:
                warn.append("invalid price")

        # Transfer constraints
        tags = set(_get(f, "tags") or [])
        if "transfer" in tags:
            if float((_get(f, "transaction_value") or 0.0)) != 0.0:
                warn.append("transfer but transaction_value != 0")
            if _get(f, "price") is not None:
                warn.append("transfer but price is not None")

        return warn
