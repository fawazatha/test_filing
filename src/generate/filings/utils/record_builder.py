from __future__ import annotations
from typing import Any, Dict, Optional
import json, uuid, hashlib

from .company_info import CompanyInfoProvider
from .formats import (
    string_to_slug, fmt_num_as_str, last_segment, ensure_symbol_suffix,
    timestamp_to_output, dump_model, build_title, build_body
)
from .aggregations import extract_transactions, choose_direction_and_legs, weighted_avg, inconsistent


def _extract_existing_uid(f: Dict[str, Any]) -> Optional[str]:
    for path in [
        ("UID",),
        ("link", "uid"),
        ("extra", "uid"),
    ]:
        cur = f
        ok = True
        for p in path:
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            else:
                ok = False
                break
        if ok and cur:
            return str(cur)
    return None


def _lookup_company_info_with_variants(provider: CompanyInfoProvider, symbol_from_parsed: str) -> Optional[Dict[str, str]]:
    candidates: list[str] = []
    s = (symbol_from_parsed or "").strip()
    if s:
        up = s.upper()
        # base (e.g., ISSP)
        candidates.append(up)
        # full (e.g., ISSP.JK)
        up_with = ensure_symbol_suffix(up)
        if up_with not in candidates:
            candidates.append(up_with)
        # also accept stripping .JK if already there
        up_base = up.replace(".JK", "")
        if up_base and up_base not in candidates:
            candidates.append(up_base)

    for cand in candidates:
        info = provider.get_company_info(cand)
        if info:
            return info
    return None


def build_output_record(filing: Any, downloads_map: Dict[str, Dict], provider: CompanyInfoProvider) -> Dict[str, Any]:
    f = dump_model(filing)

    # --- NEW: resolve symbol from downloads_map when missing (NON-IDX case) ---
    # We key by the PDF filename in parsed "source".
    src_field = f.get("source") or ""          # may be filename or URL
    filename = last_segment(src_field)
    dl_meta = downloads_map.get(filename, {})
    ticker_from_dl = (dl_meta.get("ticker") or "").strip().upper()

    # prefer parsed symbol; else fall back to ticker_from_dl
    symbol_raw = (f.get("symbol") or "").strip().upper()
    if not symbol_raw and ticker_from_dl:
        symbol_raw = ticker_from_dl

    # symbol/company for output: we always add .JK suffix on output symbol
    symbol_out = ensure_symbol_suffix(symbol_raw)
    company    = symbol_out.replace(".JK", "")

    # tx legs & direction
    txns = extract_transactions(filing)
    direction, legs = choose_direction_and_legs(txns)

    prices = [t.get("price") for t in legs if t.get("price") is not None]
    amts   = [t.get("amount") for t in legs if t.get("amount") is not None]
    vals   = [t.get("value") for t in legs if t.get("value") is not None]

    price = 0.0
    if prices and amts and len(prices) == len(amts):
        price = weighted_avg(prices, amts)
    elif vals and amts and sum(int(a or 0) for a in amts) > 0:
        price = (sum(float(v or 0.0) for v in vals) / sum(int(a or 0) for a in amts))

    amount_transaction = sum(int(a or 0) for a in amts) if amts else int(f.get("extra", {}).get("_raw", {}).get("amount_transaction") or 0)
    transaction_value  = sum(float(v or 0.0) for v in vals) if vals else float(f.get("extra", {}).get("_raw", {}).get("transaction_value") or 0.0)

    # holdings & share % (string outputs handled later)
    holding_before = int(f.get("holding_before") or 0)
    holding_after  = int(f.get("holding_after")  or 0)
    sp_before = f.get("share_percentage_before")
    sp_after  = f.get("share_percentage_after")
    sp_trx    = f.get("extra", {}).get("share_percentage_transaction")
    if sp_trx is None and (sp_before is not None and sp_after is not None):
        try:
            sp_trx = float(sp_after) - float(sp_before)
        except Exception:
            sp_trx = None

    # link & timestamp via filename (still using downloads_map)
    source_url = dl_meta.get("url") or src_field
    timestamp  = timestamp_to_output(dl_meta.get("timestamp"))

    # holder
    holder_name = f.get("holder_name") or ""
    holder_type = f.get("holder_type")

    # sector/sub_sector via provider (try many symbol variants), fallback to data on filing
    sector = sub_sector = None
    info = _lookup_company_info_with_variants(provider, symbol_raw)
    if info:
        sector = string_to_slug(info.get("sector") or "")
        sub_sector = string_to_slug(info.get("sub_sector") or "")
    else:
        sector = string_to_slug(f.get("sector") or f.get("extra", {}).get("sector") or "")
        sub_sector = string_to_slug(
            f.get("sub_sector") or f.get("extra", {}).get("sub_sector") or ""
        )

    # Build a stable UID (reuse if exists)
    uid = _extract_existing_uid(f) or hashlib.sha1(
        json.dumps(
            {
                "symbol": symbol_out,
                "holder": holder_name,
                "amount": amount_transaction,
                "ts": timestamp or "",
                "src": filename or source_url,
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:20]

    # price_transaction (store raw arrays if available)
    price_transaction_out = {
        "prices": [p for p in prices] if prices else None,
        "amounts": [a for a in amts] if amts else None,
        "values": [v for v in vals] if vals else None,
    }

    # Final JSON record (strings where expected)
    spb_out = None if sp_before is None else str(sp_before)
    spa_out = None if sp_after  is None else str(sp_after)
    spt_out = None if sp_trx    is None else str(sp_trx)

    return {
        "title": build_title(holder_name, direction, company),
        "body": build_body(timestamp or "-", holder_name, holder_type, direction, amount_transaction, company, holding_before, holding_after),

        "symbol": symbol_out,
        "transaction_type": direction,
        "holding_before": str(holding_before),
        "holding_after": str(holding_after),
        "amount_transaction": str(amount_transaction),
        "holder_type": holder_type,
        "holder_name": holder_name,
        "price": fmt_num_as_str(price),
        "transaction_value": fmt_num_as_str(transaction_value),
        "price_transaction": price_transaction_out,
        "share_percentage_before": spb_out,
        "share_percentage_after": spa_out,
        "share_percentage_transaction": spt_out,
        "UID": uid,

        # provenance-ish
        "link": {
            "uid": uid,
            "url": source_url,
            "filename": filename,
            "timestamp": timestamp,
        },
        "sector": sector,
        "sub_sector": sub_sector,
    }
