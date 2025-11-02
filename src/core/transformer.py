# src/core/transformer.py
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from src.core.types import FilingRecord, PriceTransaction
from src.common.strings import to_float, to_int, kebab, strip_diacritics  # noqa: F401

# Tag dictionaries / mappings
# Keep whitelist minimal; 'share-transfer' will be gated by tx_type.
TAG_WHITELIST = {
    "takeover", "mesop", "inheritance", "award",
    "share-transfer", "internal-strategy",
}

PURPOSE_TAG_MAP = {
    "akuisisi": "takeover", "acquisition": "takeover",
    "strategi internal": "internal-strategy", "internal strategy": "internal-strategy",
    "pengembangan usaha": "internal-strategy", "business expansion": "internal-strategy",
    "mesop": "mesop", "warisan": "inheritance", "inheritance": "inheritance",
    "penghargaan": "award", "award": "award",
    # Only allow 'share-transfer' if tx_type == 'share-transfer' (guard in _normalize_tags).
    "transfer": "share-transfer",
}

# Small helpers
def _translate_to_english(text: str) -> str:
    """Tiny phrase-level translator for titles/bodies and tag inference."""
    if not text:
        return ""
    known = {
        "bagian dari proses akuisisi": "Part of the acquisition process",
        "strategi internal": "Internal strategy",
        "pengembangan usaha": "Business expansion",
        "investasi": "investment",
        "divestasi": "divestment",
    }
    s = text.strip().lower()
    if s in known:
        return known[s]
    if s == "investation":
        return "Investment"
    logging.warning(f"No translation found for '{text}'. Using original.")
    return text

def _to_str(x: Any) -> Optional[str]:
    """Coerce any value to a trimmed string; return None for null/empty."""
    if x is None:
        return None
    s = str(x).strip()
    return s if s != "" else None

def _parse_date_obj(x: Any) -> Optional[datetime]:
    """Best-effort parser for YYYYMMDD or ISO-like strings."""
    if x is None or x == "":
        return None
    if isinstance(x, datetime):
        return x
    s = str(x).strip()

    # Try YYYYMMDD
    if len(s) == 8 and s.isdigit():
        try:
            return datetime.strptime(s, "%Y%m%d")
        except Exception:
            pass

    # Try ISO (accept a space between date/time)
    try:
        return datetime.fromisoformat(s.replace(" ", "T"))
    except Exception:
        pass

    logging.warning(f"Could not parse date: {x}. Returning None.")
    return None

def _to_iso_date_full(x: Any) -> Optional[str]:
    """Return ISO datetime (YYYY-MM-DDTHH:MM:SS[.ffff]) or None."""
    dt = _parse_date_obj(x)
    return dt.isoformat() if dt else None

def _to_iso_date_short(x: Any) -> Optional[str]:
    """Return ISO date only (YYYY-MM-DD) or None."""
    dt = _parse_date_obj(x)
    return dt.strftime("%Y-%m-%d") if dt else None

def _ensure_date_yyyy_mm_dd(s: Optional[str]) -> Optional[str]:
    """Accept 'YYYY-MM-DD' or 'YYYY-MM-DDTHH:MM:SS[Z]' and trim to date."""
    if not s:
        return None
    return str(s)[:10]

def _normalize_symbol(sym: Any) -> Optional[str]:
    """Normalize to UPPER + '.JK' suffix if missing."""
    s = _to_str(sym)
    if not s:
        return None
    s = s.upper()
    return s if s.endswith(".JK") else f"{s}.JK"

# Core transaction logic
def _normalize_transaction_type(raw_type: Any, holding_before: Any, holding_after: Any) -> str:
    """
    Prefer explicit type; otherwise infer buy/sell from holding delta.
    If still unknown (incl. strings that contain 'transfer'), fall back to 'other'.
    """
    t = (_to_str(raw_type) or "").lower()
    if t in {"buy", "sell", "share-transfer", "award", "inheritance", "mesop"}:
        return t

    hb = to_int(holding_before)
    ha = to_int(holding_after)
    if hb is not None and ha is not None:
        if ha > hb:
            return "buy"
        if ha < hb:
            return "sell"

    # Any uncertain/ambiguous descriptor defaults to 'other'
    # (including strings containing 'transfer' but not explicitly recognized)
    return "other"

def _build_tx_list_from_list(tx_list: List[Dict[str, Any]], raw_date: Any) -> List[PriceTransaction]:
    """New input format: list of dicts → List[PriceTransaction]."""
    out: List[PriceTransaction] = []
    for tx in tx_list or []:
        if not isinstance(tx, dict):
            continue
        out.append(PriceTransaction(
            transaction_date=_to_iso_date_short(tx.get("date") or raw_date),
            transaction_type=_to_str(tx.get("type")),
            transaction_price=to_float(tx.get("price")),
            transaction_share_amount=to_int(tx.get("amount") or tx.get("amount_transacted")),
        ))
    return out

def _build_tx_list_from_dict(tx_dict: Dict[str, Any], raw_date: Any) -> List[PriceTransaction]:
    """Legacy input format: dict of lists (prices, amount_transacted, type) → List[PriceTransaction]."""
    out: List[PriceTransaction] = []
    try:
        prices = tx_dict.get("prices", [])
        amounts = tx_dict.get("amount_transacted", [])
        types = tx_dict.get("type", [])
        # Normalize 'type' to list if a single string is provided
        if isinstance(types, str):
            types = [types] * max(len(prices), len(amounts), 1)

        max_len = max(len(prices), len(amounts), len(types) if isinstance(types, list) else 0)
        for i in range(max_len):
            out.append(PriceTransaction(
                transaction_date=_to_iso_date_short(raw_date),
                transaction_type=_to_str(types[i]) if isinstance(types, list) and i < len(types) else "other",
                transaction_price=to_float(prices[i]) if i < len(prices) else None,
                transaction_share_amount=to_int(amounts[i]) if i < len(amounts) else None,
            ))
        return out
    except Exception:
        return []

def _calculate_wap_and_totals(tx_list: List[PriceTransaction]) -> Tuple[Optional[float], Optional[int], Optional[float]]:
    """Compute weighted average price and totals from buy/sell transactions only."""
    total_value, total_amount = 0.0, 0
    for tx in tx_list:
        price, amount = tx.transaction_price, tx.transaction_share_amount
        tx_type = (tx.transaction_type or "").lower()
        if tx_type in {"buy", "sell"}:
            if price is not None and amount is not None and price > 0 and amount > 0:
                total_value += price * amount
                total_amount += amount
    if total_amount == 0:
        return None, None, None
    return total_value / total_amount, total_amount, total_value

def _generate_title_and_body(
    holder_name: str,
    company_name: str,
    tx_type: str,
    amount: Optional[int],
    holding_before: Optional[int],
    holding_after: Optional[int],
    purpose_en: str,
) -> tuple[str, str]:
    """Human-friendly title/body with minimal grammar rules."""
    action_title = tx_type.replace("-", " ").title()
    if tx_type == "buy":
        action_verb = "bought"
    elif tx_type == "sell":
        action_verb = "sold"
    elif tx_type == "share-transfer":
        action_verb = "transferred"
    elif tx_type == "award":
        action_verb = "was awarded"
    elif tx_type == "inheritance":
        action_verb = "inherited"
    else:
        action_verb = "executed a transaction for"

    title = f"{holder_name} {action_title} Transaction of {company_name}"
    amount_str = f"{amount:,} shares" if amount is not None else "shares"
    body = f"{holder_name} {action_verb} {amount_str} of {company_name}."

    if holding_before is not None and holding_after is not None:
        hb_str, ha_str = f"{holding_before:,}", f"{holding_after:,}"
        if holding_after > holding_before:
            body += f" This increases their holdings from {hb_str} to {ha_str} shares."
        elif holding_after < holding_before:
            body += f" This decreases their holdings from {hb_str} to {ha_str} shares."
        else:
            body += f" Their holdings remain at {ha_str} shares."

    if purpose_en:
        body += f" The stated purpose of the transaction was {purpose_en.lower()}."
    return title, body

def _enrich_sector_from_map(
    symbol: Optional[str],
    raw_sector: Any,
    raw_sub_sector: Any,
    company_map: Optional[Dict[str, Dict[str, Any]]],
) -> Tuple[str, str]:
    """
    Fill sector/sub_sector from company_map if raw is empty/unknown.
    Always return kebab-case strings (never None) to satisfy NOT NULL DB constraints.
    """
    sec = _to_str(raw_sector)
    sub = _to_str(raw_sub_sector)

    need_lookup = (not sec or sec.lower() == "unknown") or (not sub or sub.lower() == "unknown")

    if need_lookup and company_map and symbol:
        sym_up = symbol.upper()
        info = company_map.get(sym_up)
        if not info:
            # try without .JK or with .JK, depending on how keys are stored
            base = sym_up.removesuffix(".JK")
            info = company_map.get(base) or company_map.get(f"{base}.JK")
        if info:
            sec = sec or _to_str(info.get("sector"))
            sub = sub or _to_str(info.get("sub_sector"))

    # Fallback to 'unknown' to avoid NULL insert errors on DB
    sec_out = kebab(sec) if sec else "unknown"
    sub_out = kebab(sub) if sub else "unknown"
    return sec_out, sub_out

def _normalize_tags(raw_tags: Any, purpose_en: str, tx_type: str) -> List[str]:
    """
    Normalize tags:
      - keep only items in whitelist,
      - allow 'share-transfer' ONLY if tx_type == 'share-transfer',
      - add tags derived from purpose (with the same guard for share-transfer),
      - optionally ensure the main tx_type (if whitelisted) is in the final set.
    """
    tags = set()
    tag_list: List[str] = []
    if isinstance(raw_tags, list):
        tag_list = raw_tags
    elif isinstance(raw_tags, str):
        try:
            tag_list = json.loads(raw_tags)
        except Exception:
            tag_list = [t.strip() for t in raw_tags.split(",")]

    tx_low = (tx_type or "").lower()

    for tag in tag_list:
        t_low = (_to_str(tag) or "").lower()
        if not t_low:
            continue
        if t_low == "share-transfer" and tx_low != "share-transfer":
            # prevent leaking 'share-transfer' when the main tx is not transfer
            continue
        if t_low in TAG_WHITELIST:
            tags.add(t_low)

    purpose_low = (purpose_en or "").lower()
    for key, mapped in PURPOSE_TAG_MAP.items():
        if key in purpose_low:
            if mapped == "share-transfer" and tx_low != "share-transfer":
                # same guard for purpose-derived tag
                continue
            tags.add(mapped)

    # Ensure primary tx_type is reflected if it's in the whitelist
    if tx_low in TAG_WHITELIST:
        tags.add(tx_low)

    return sorted(tags)

# Public API
def transform_raw_to_record(
    raw_dict: Dict[str, Any],
    ingestion_map: Dict[str, Dict[str, Any]],
    company_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> FilingRecord:
    """
    Convert arbitrary parsed dict to canonical FilingRecord.

    Notes
    -----
    - Keeps price_transaction as List[PriceTransaction] (internal format),
      so downstream processors can safely access tx.transaction_price, etc.
    - Sector/sub_sector are enriched from company_map when missing/unknown.
    """
    # Purpose (translated)
    raw_purpose = _to_str(raw_dict.get("purpose"))
    purpose_en = _translate_to_english(raw_purpose)

    # Holdings & transaction type
    holding_before = to_int(raw_dict.get("holding_before"))
    holding_after = to_int(raw_dict.get("holding_after"))
    tx_type = _normalize_transaction_type(
        raw_dict.get("transaction_type") or raw_dict.get("type"),
        holding_before, holding_after
    )

    # Percentages
    pp_before = to_float(raw_dict.get("share_percentage_before"), ndigits=5)
    pp_after  = to_float(raw_dict.get("share_percentage_after"),  ndigits=5)
    pp_tx     = to_float(raw_dict.get("share_percentage_transaction"), ndigits=5)

    # Timestamp & source (priority: ingestion_map → parser → first tx)
    main_date: Optional[str] = None
    main_source_url: Optional[str] = None
    raw_filename = _to_str(raw_dict.get("source"))

    if raw_filename:
        ingestion_item = ingestion_map.get(raw_filename)
        if ingestion_item:
            main_date = ingestion_item.get("date")  # "YYYY-MM-DDTHH:MM:SS"
            main_source_url = ingestion_item.get("main_link") or ingestion_item.get("link")

    if not main_date:
        main_date = raw_dict.get("timestamp") or raw_dict.get("announcement_published_at")

    if not main_date:
        txs_for_date = raw_dict.get("transactions")
        if isinstance(txs_for_date, list) and txs_for_date:
            main_date = txs_for_date[0].get("date")

    if not main_source_url:
        main_source_url = _to_str(raw_dict.get("source") or raw_dict.get("pdf_url"))

    # Build INTERNAL transaction list
    price_tx_list: List[PriceTransaction] = []
    if isinstance(raw_dict.get("transactions"), list):
        price_tx_list = _build_tx_list_from_list(raw_dict["transactions"], main_date)
    elif isinstance(raw_dict.get("price_transaction"), dict):
        price_tx_list = _build_tx_list_from_dict(raw_dict["price_transaction"], main_date)

    # Aggregate price/amount/value (WAP if price missing)
    wap, total_amount_tx, total_value_tx = _calculate_wap_and_totals(price_tx_list)

    amount = to_int(raw_dict.get("amount_transaction") or raw_dict.get("amount"))
    if amount is None:
        amount = total_amount_tx

    value = to_float(raw_dict.get("transaction_value"))
    if value is None:
        value = total_value_tx

    price = to_float(raw_dict.get("price"))
    if price is None and wap is not None:
        price = wap

    if value is None and price is not None and amount is not None:
        value = price * amount

    # Human-friendly content
    holder_name = _to_str(raw_dict.get("holder_name")) or "Unknown Shareholder"
    company_name = _to_str(
        raw_dict.get("company_name_raw") or
        raw_dict.get("company_name") or
        raw_dict.get("symbol")
    ) or "Unknown Company"

    title, body = _generate_title_and_body(
        holder_name, company_name, tx_type, amount,
        holding_before, holding_after, purpose_en
    )

    # Tags (guard 'share-transfer' with tx_type)
    tags = _normalize_tags(raw_dict.get("tags"), purpose_en, tx_type)

    # Symbol + sectors (enrich from company_map if needed)
    symbol_norm = _normalize_symbol(raw_dict.get("symbol") or raw_dict.get("issuer_code"))
    sec_raw = raw_dict.get("sector")
    sub_raw = raw_dict.get("sub_sector")
    sector, sub_sector = _enrich_sector_from_map(symbol_norm, sec_raw, sub_raw, company_map)

    record = FilingRecord(
        symbol=symbol_norm,
        timestamp=_to_iso_date_full(main_date),
        transaction_type=tx_type,
        holder_name=holder_name,

        holding_before=holding_before,
        holding_after=holding_after,
        amount_transaction=amount,

        share_percentage_before=pp_before,
        share_percentage_after=pp_after,
        share_percentage_transaction=pp_tx,

        price=price,
        transaction_value=value,

        title=title,
        body=body,
        purpose_of_transaction=purpose_en,

        # Keep INTERNAL representation; to_db_dict() will serialize properly.
        price_transaction=price_tx_list,

        tags=tags,
        sector=sector,
        sub_sector=sub_sector,

        source=main_source_url,
        holder_type=_to_str(raw_dict.get("holder_type")),

        raw_data=raw_dict,
    )

    # For alerting later in processors.py:
    # mark transfer/other rows for manual UID pairing checks.
    if tx_type in {"share-transfer", "other"}:
        record.audit_flags["needs_manual_uid"] = True
        record.audit_flags["reason"] = "manual UID check recommended for transfer/other"

    return record

def transform_many(
    raw_dicts: List[Dict[str, Any]],
    ingestion_map: Dict[str, Dict[str, Any]],
    company_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[FilingRecord]:
    """Vector wrapper for transform_raw_to_record."""
    out: List[FilingRecord] = []
    for raw in (raw_dicts or []):
        if not isinstance(raw, dict):
            continue
        try:
            out.append(transform_raw_to_record(raw, ingestion_map, company_map=company_map))
        except Exception as e:
            logging.error(f"Failed to transform row: {e}. Row: {raw}", exc_info=True)
    return out
