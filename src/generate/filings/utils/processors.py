from __future__ import annotations
import json, uuid, logging
from typing import Any, Dict, List, Tuple

from ..types import CompanyInfo
from ..config import PRICE_DEVIATION_THRESHOLD, VALUE_KEEP_THRESHOLD, PCT_KEEP_THRESHOLD
from .normalizers import (
    normalize_symbol, parse_timestamp, safe_int, safe_float, slug
)
from .consolidators import determine_transaction_type_from_list, average_price
from .validators import is_direction_consistent, build_tags
from .provider import get_latest_price, get_company_info

logger = logging.getLogger(__name__)

def compute_estimated_value(parsed_price: float|None,
                            transaction_value: float|None,
                            amount_transacted: int,
                            latest_price: float|None) -> float:
    if transaction_value is not None and transaction_value > 0:
        return float(transaction_value)
    if parsed_price and amount_transacted:
        return float(parsed_price) * amount_transacted
    if latest_price is not None and amount_transacted:
        return latest_price * amount_transacted
    return 0.0

import uuid, re

def to_full_symbol(sym: str | None) -> str:
    s = (sym or "").upper().strip()
    return s if s.endswith(".JK") else (s + ".JK")

_DATE_HEAD = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")  

def _extract_yyyymmdd(ts_str: str | None) -> str:
    if not ts_str:
        return ""
    s = ts_str.strip()

    try:
        if "T" in s:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).strftime("%Y%m%d")
    except Exception:
        pass
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d")
    except Exception:
        parse_timestamp
    m = _DATE_HEAD.match(s)
    if m:
        return "".join(m.groups())
    
    digits = re.sub(r"\D", "", s)
    return digits[:8]

def maybe_generate_uid(symbol: str | None, ts_str: str | None, *args: Any) -> str:
    if len(args) == 0:
        amount = 0
    elif len(args) == 1:
        amount = args[0]                 
    else:
        amount = args[1]

    sym_full = to_full_symbol(symbol).lower()
    yyyymmdd = _extract_yyyymmdd(ts_str)
    try:
        amt = abs(int(amount or 0))
    except Exception:
        amt = abs(int(re.sub(r"[^\d-]", "", str(amount or "0")) or 0))

    seed = f"{sym_full}|{yyyymmdd}|{amt}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, seed))

def enrich_and_filter_items(
    parsed_items: List[Dict[str, Any]],
    download_map: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:

    results: List[Dict[str, Any]] = []
    alerts: List[Dict[str, Any]] = []

    for item in parsed_items:
        source_file = (item.get("source") or "").split("/")[-1]
        meta = download_map.get(source_file) if source_file else None
        url = meta.url if meta else None
        ts_from_downloads = meta.timestamp if meta else None

        symbol_full = normalize_symbol(item.get("symbol"), item.get("issuer_code"))
        if not symbol_full:
            logger.debug("Skip item without symbol: %s", item.get("holder_name"))
            continue

        company_row = get_company_info(symbol_full) or CompanyInfo(company_name=item.get("company_name") or item.get("company_name_raw") or "")
        company_name = company_row.company_name or item.get("company_name") or item.get("company_name_raw") or ""

        txs: List[Dict[str, Any]] = item.get("transactions") or []
        tx_type = (item.get("transaction_type") or "").lower()
        parsed_price = item.get("price")
        parsed_tx_value = item.get("transaction_value")

        if not tx_type and txs:
            tx_type, net_value, main_txs = determine_transaction_type_from_list(txs)
            parsed_tx_value = parsed_tx_value or net_value
            item["amount_transacted"] = sum(safe_int(t.get("amount")) for t in main_txs)
            parsed_price = average_price(main_txs) if item["amount_transacted"] else 0.0
            # sync share/holding
            item["holding_before"] = txs[0].get("holding_before", item.get("holding_before"))
            item["holding_after"] = txs[-1].get("holding_after", item.get("holding_after"))
            item["share_percentage_before"] = txs[0].get("share_percentage_before", item.get("share_percentage_before"))
            item["share_percentage_after"] = txs[-1].get("share_percentage_after", item.get("share_percentage_after"))

        if tx_type in ("buy","sell") and txs and len(txs)==1 and not item.get("price_transaction"):
            item["price_transaction"] = {
                "prices": [txs[0].get("price")],
                "amount_transacted": [txs[0].get("amount")],
            }

        amount_transacted = safe_int(item.get("amount_transacted"))
        if amount_transacted == 0:
            hb, ha = safe_int(item.get("holding_before")), safe_int(item.get("holding_after"))
            amount_transacted = abs(ha - hb)

        # infer tx_type kalau masih kosong
        if not tx_type:
            hb, ha = safe_int(item.get("holding_before")), safe_int(item.get("holding_after"))
            tx_type = "buy" if ha > hb else ("sell" if ha < hb else "other")

        before_pct = safe_float(item.get("share_percentage_before"))
        after_pct = safe_float(item.get("share_percentage_after"))
        pct_tx = abs(after_pct - before_pct)
        tags = build_tags(tx_type, before_pct, after_pct)

        # estimasi nilai
        latest_price = get_latest_price(symbol_full)
        estimated_value = compute_estimated_value(parsed_price, parsed_tx_value, amount_transacted, latest_price)
        inferred_price = (estimated_value / amount_transacted) if amount_transacted else 0.0

        # timestamp: gunakan parsed jika ada; kalau tidak ada ambil dari downloaded
        ts_primary = item.get("timestamp") or ts_from_downloads
        timestamp_db, nice_date = parse_timestamp(ts_primary)

        # konsistensi arah buy/sell vs holding delta
        hb, ha = safe_int(item.get("holding_before")), safe_int(item.get("holding_after"))
        if not is_direction_consistent(tx_type, hb, ha):
            alerts.append({
                "reason": "Inconsistent share and holding direction",
                "holder_name": (item.get("holder_name") or "").strip(),
                "transaction_type": tx_type,
                "holding_before": hb,
                "holding_after": ha,
                "share_percentage_before": before_pct,
                "share_percentage_after": after_pct,
                "UID": item.get("UID"),
                "source": url,
                "timestamp": timestamp_db,
                "company": company_name,
            })

        # alert deviasi harga
        if latest_price:
            deviation = abs(inferred_price - latest_price) / latest_price if latest_price else 0
            if deviation > PRICE_DEVIATION_THRESHOLD:
                alerts.append({
                    "reason": "Suspicious price deviation (>50%) from latest market price",
                    "holder_name": (item.get("holder_name") or "").strip(),
                    "transaction_type": tx_type,
                    "holding_before": hb,
                    "holding_after": ha,
                    "share_percentage_before": before_pct,
                    "share_percentage_after": after_pct,
                    "UID": item.get("UID"),
                    "source": url,
                    "timestamp": timestamp_db,
                    "company": company_name,
                    "inferred_price": round(inferred_price, 2),
                    "latest_price": latest_price,
                    "deviation": f"{deviation:.2%}",
                    "amount_transacted": amount_transacted,
                    "transaction_value": estimated_value,
                    "price_transaction": item.get("price_transaction", {}),
                })

        # UID hanya untuk transfer
        uid = item.get("UID")
        if (item.get("is_transfer") is True) and not uid:
            uid = maybe_generate_uid(symbol_full, ts_primary, parsed_price, amount_transacted)

        # filter final (sama seperti legacy): nilai atau delta %
        passes_threshold = (estimated_value > VALUE_KEEP_THRESHOLD) or (pct_tx > PCT_KEEP_THRESHOLD)

        holder = (item.get("holder_name") or "").replace("\n", " ").strip()
        tx_verb = ({"buy":"bought","sell":"sold","transfer":"transferred","other":"transferred"}
                   .get(tx_type, f"{tx_type}ed")).lower()

        filing = {
            "title": f"{holder} {tx_type.capitalize()} Transaction of {company_name}",
            "body": f"On {nice_date}, {holder}, an {item.get('holder_type')} shareholder, "
                    f"{tx_verb} {amount_transacted:,} shares of {company_name}, changing its holding "
                    f"from {hb:,} to {ha:,} shares.",
            "source": url,
            "timestamp": timestamp_db,  # <- sekarang bisa berasal dari parsed atau downloaded
            "sector": slug(company_row.sector or "") if company_row.sector else "",
            "sub_sector": slug(company_row.sub_sector or "") if company_row.sub_sector else "",
            "tags": json.dumps(tags),
            "symbol": symbol_full,
            "transaction_type": tx_type or None,
            "holding_before": str(hb),
            "holding_after": str(ha),
            "amount_transaction": str(amount_transacted),
            "holder_type": item.get("holder_type"),
            "holder_name": holder,
            "price": str(parsed_price if parsed_price is not None else 0.0),
            "transaction_value": str(estimated_value),
            "price_transaction": json.dumps(item.get("price_transaction", {})),
            "share_percentage_before": f"{before_pct:.3f}",
            "share_percentage_after": f"{after_pct:.3f}",
            "share_percentage_transaction": f"{pct_tx:.3f}",
            "UID": uid,
        }

        if passes_threshold:
            results.append(filing)
        else:
            logger.debug("[SKIP] %s → Rp%s, Δ%s%%", holder, f"{estimated_value:,.0f}", f"{pct_tx:.3f}")

    return results, alerts
