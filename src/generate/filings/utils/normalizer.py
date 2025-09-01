from __future__ import annotations
from typing import Dict, Any, List, Optional
import hashlib

from ..models import make_filing, make_transaction, make_linking_meta

try:
    from src.utils.logger import get_logger  # type: ignore
except Exception:
    import logging
    def get_logger(name: str, verbose: bool = False):
        lvl = logging.DEBUG if verbose else logging.INFO
        logging.basicConfig(level=lvl, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
        return logging.getLogger(name)

log = get_logger("generator.filings.normalizer")

def _s(v: Optional[str]) -> str:
    return v or ""

def _to_int(v: Any) -> Optional[int]:
    try:
        if v is None: return None
        return int(v)
    except Exception:
        return None

def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None: return None
        return float(v)
    except Exception:
        return None

def _sha1_uid(*parts: Any) -> str:
    s = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def normalize_idx_record(rec: Dict[str, Any]):
    symbol = _s(rec.get("ticker")) or _s(rec.get("issuer_symbol"))
    source = _s(rec.get("source"))

    legs: List[Any] = []
    for leg in rec.get("transactions", []) or []:
        legs.append(make_transaction(
            type=_s(leg.get("type")) or "buy",
            price=_to_float(leg.get("price")) or 0.0,
            amount=_to_int(leg.get("amount")) or 0,
            value=_to_float(leg.get("value")) or 0.0,
            holding_before=None,
            holding_after=None,
            share_percentage_before=None,
            share_percentage_after=None,
            share_percentage_transaction=None,
        ))

    total_value = sum(getattr(t, "value", t.get("value", 0.0)) for t in legs)
    total_amount = sum(getattr(t, "amount", t.get("amount", 0)) for t in legs) or (_to_int(rec.get("amount_transacted")) or 0)

    uid = _sha1_uid(symbol, rec.get("holder_name"), source, total_amount, total_value)

    return make_filing(
        source=source,
        symbol=symbol,
        date="",  # fill later if you join with announcements
        holder_name=_s(rec.get("holder_name")),
        holder_type=_s(rec.get("holder_type")) or None,
        holding_before=_to_int(rec.get("holding_before")) or 0,
        holding_after=_to_int(rec.get("holding_after")) or 0,
        share_percentage_before=_to_float(rec.get("share_percentage_before")) or 0.0,
        share_percentage_after=_to_float(rec.get("share_percentage_after")) or 0.0,
        transactions=legs,
        transaction_value=total_value,
        sector=None,
        subsector=None,
        link=make_linking_meta(uid=uid),
        extra={
            "company_name": rec.get("company_name"),
            "issuer_symbol": rec.get("issuer_symbol"),
            "holder_symbol": rec.get("holder_symbol"),
            "is_transfer": bool(rec.get("is_transfer", False)),
            "price": rec.get("price"),
            "price_transaction": rec.get("price_transaction"),
            "share_percentage_transaction": rec.get("share_percentage_transaction"),
            "_raw": {k: rec.get(k) for k in ["transaction_type", "amount_transacted", "transaction_value"]},
        },
    )

def normalize_non_idx_record(rec: Dict[str, Any]):
    symbol = _s(rec.get("symbol"))
    source = _s(rec.get("source"))

    amt = _to_int(rec.get("amount_transaction"))
    if amt is None:
        b = _to_int(rec.get("holding_before"))
        a = _to_int(rec.get("holding_after"))
        if b is not None and a is not None:
            try: amt = abs(a - b)
            except Exception: amt = None
    amt = amt or 0

    price = _to_float(rec.get("price")) or 0.0
    value = _to_float(rec.get("transaction_value")) or 0.0
    leg_type = _s(rec.get("transaction_type")) or "buy"

    legs = [make_transaction(
        type=leg_type if leg_type in ("buy", "sell", "transfer") else "buy",
        price=price,
        amount=amt,
        value=value,
        holding_before=None,
        holding_after=None,
        share_percentage_before=None,
        share_percentage_after=None,
        share_percentage_transaction=_to_float(rec.get("share_percentage_transaction")),
    )]

    uid = _sha1_uid(symbol, rec.get("holder_name"), source, amt, value)

    return make_filing(
        source=source,
        symbol=symbol,
        date="",
        holder_name=_s(rec.get("holder_name")),
        holder_type=_s(rec.get("holder_type")) or None,
        holding_before=_to_int(rec.get("holding_before")) or 0,
        holding_after=_to_int(rec.get("holding_after")) or 0,
        share_percentage_before=_to_float(rec.get("share_percentage_before")) or 0.0,
        share_percentage_after=_to_float(rec.get("share_percentage_after")) or 0.0,
        transactions=legs,
        transaction_value=value or sum(getattr(t, "value", t.get("value", 0.0)) for t in legs),
        sector=None,
        subsector=None,
        link=make_linking_meta(uid=uid),
        extra={
            "title": rec.get("title"),
            "body": rec.get("body"),
            "tags": rec.get("tags") or [],
            "price_transaction": rec.get("price_transaction"),
            "UID_raw": rec.get("UID"),
            "_raw_date": rec.get("timestamp"),
        },
    )
