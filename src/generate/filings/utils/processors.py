from __future__ import annotations
import json, uuid, logging, os, re
from datetime import datetime
from typing import Any, Dict, List, Tuple

from ..types import CompanyInfo
from ..config import PRICE_DEVIATION_THRESHOLD, VALUE_KEEP_THRESHOLD, PCT_KEEP_THRESHOLD
from .normalizers import (
    normalize_symbol, parse_timestamp, safe_int, safe_float, slug
)
from .consolidators import determine_transaction_type_from_list, average_price
from .provider import get_latest_price, get_company_info, get_tags

logger = logging.getLogger(__name__)

# ------------------------------
# Alert config & writers
# ------------------------------
CORRECTION_ALERT_PATH = "alerts/correction_filings.json"
MIX_TRANSFER_ALERT_PATH = "alerts/mix_transfer.json"
_CORRECTION_KEYWORDS = ("CORRECTION", "KOREKSI")


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
        pass
    m = _DATE_HEAD.match(s)
    if m:
        return "".join(m.groups())

    digits = re.sub(r"\D", "", s)
    return digits[:8]


def maybe_generate_uid(symbol: str | None, ts_str: str | None, *args: Any) -> str:
    # Amount priority: (price, amount) → use amount; (amount,) → use amount; () → 0
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


def is_correction_title(title: str | None) -> bool:
    if not title:
        return False
    t = title.upper()
    return any(k in t for k in _CORRECTION_KEYWORDS)


def _write_json_alerts(path: str, new_alerts: List[Dict[str, Any]],
                       key_fields: List[str]) -> None:
    if not new_alerts:
        return
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        existing: List[Dict[str, Any]] = []
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                try:
                    existing = json.load(f) or []
                except Exception:
                    existing = []

        def _key(a: Dict[str, Any]) -> str:
            return "|".join(str(a.get(k) or "") for k in key_fields)

        seen = set()
        merged: List[Dict[str, Any]] = []
        for a in existing + new_alerts:
            k = _key(a)
            if k in seen:
                continue
            seen.add(k)
            merged.append(a)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)
        logger.info("Wrote %d alert(s) to %s", len(new_alerts), path)
    except Exception as e:
        logger.error("Failed to write alerts to %s: %s", path, e)


def _write_correction_alerts(new_alerts: List[Dict[str, Any]]) -> None:
    _write_json_alerts(
        CORRECTION_ALERT_PATH,
        new_alerts,
        key_fields=["UID", "source", "timestamp"],
    )


def _write_mix_transfer_alerts(new_alerts: List[Dict[str, Any]]) -> None:
    _write_json_alerts(
        MIX_TRANSFER_ALERT_PATH,
        new_alerts,
        key_fields=["symbol", "holder_name", "timestamp", "source"],
    )


# ------------------------------
# Helpers & normalizers
# ------------------------------
def _to_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]

def _is_transfer_row(t: Dict[str, Any]) -> bool:
    typ = str(t.get("type") or "").strip().lower()
    return typ in {"transfer"}  # add synonyms if needed

def _holder_text(item: Dict[str, Any]) -> str:
    """Prefer holder_name_raw; fallback ke holder_name. Rapikan newline & spasi."""
    return (item.get("holder_name_raw") or item.get("holder_name") or "")\
        .replace("\n", " ").strip()


def normalize_price_transaction(
    item: Dict[str, Any],
    tx_type: str | None,
    txs: List[Dict[str, Any]] | None = None,
) -> Dict[str, List[Any]]:
    """
    Normalisasi price_transaction → {type[], prices[], amount_transacted[]}
    Aturan:
      - Terima price_transaction sebagai dict atau string JSON.
      - Jika 'type' hilang tapi ada transactions, ambil tipe per baris dari transactions.
      - Jika prices/amounts kosong tapi ada transactions, isi dari transactions.
      - Panjang list diselaraskan (pad/truncate) dengan prioritas data yang ada.
    """
    import json as _json

    txs = txs or []

    # 1) Ambil raw PT (boleh string JSON)
    pt_raw = item.get("price_transaction") or {}
    if isinstance(pt_raw, str):
        try:
            pt_raw = _json.loads(pt_raw) or {}
        except Exception:
            pt_raw = {}

    # 2) Baca kolom-kolom yang mungkin dipakai
    types_in   = pt_raw.get("type") or pt_raw.get("types")
    prices_in  = pt_raw.get("prices") or pt_raw.get("price")
    amts_in    = pt_raw.get("amount_transacted") or pt_raw.get("amounts") or pt_raw.get("amount")

    def _t_lower(x):
        return (str(x).strip().lower() if x is not None else None)

    # 3) Seed dari PT raw
    types   = [_t_lower(t) for t in _to_list(types_in)]
    prices  = [safe_float(p) if p is not None else None for p in _to_list(prices_in)]
    amounts = [safe_int(a)   if a is not None else 0    for a in _to_list(amts_in)]

    # 4) Kalau prices/amounts kosong tapi ada transactions → isi dari transactions
    if not prices and not amounts and txs:
        for t in txs:
            ttype = _t_lower(t.get("type"))
            if ttype not in {"buy", "sell", "transfer"}:
                ttype = None
            amounts.append(safe_int(t.get("amount")))
            prices.append(safe_float(t.get("price")))
            types.append(ttype)

    # 5) Kalau types masih kosong tapi ada transactions → ambil tipe per-barisan dari transactions
    if not types and txs:
        for t in txs:
            ttype = _t_lower(t.get("type"))
            if ttype not in {"buy", "sell", "transfer"}:
                ttype = None
            types.append(ttype)

    # 6) Samakan panjang ketiganya
    k = max(len(types), len(prices), len(amounts), 0)

    def _pad(lst, fill, n):
        lst = list(lst)
        if len(lst) < n:
            lst += [fill] * (n - len(lst))
        return lst[:n]

    # Prefer pad dengan informasi yang paling tepat:
    # - types → pad pakai tx_type (net) hanya jika perlu
    # - prices → pad None
    # - amounts → pad 0
    types   = _pad(types,   (tx_type if tx_type in ("buy", "sell") else None), k)
    prices  = _pad(prices,  None, k)
    amounts = _pad(amounts, 0,    k)

    return {"type": types, "prices": prices, "amount_transacted": amounts}


# ------------------------------
# Main enricher
# ------------------------------
def enrich_and_filter_items(
    parsed_items: List[Dict[str, Any]],
    download_map: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:

    results: List[Dict[str, Any]] = []
    alerts: List[Dict[str, Any]] = []
    correction_alerts: List[Dict[str, Any]] = []
    mix_transfer_alerts: List[Dict[str, Any]] = []

    for item in parsed_items:
        source_file = (item.get("source") or "").split("/")[-1]
        meta = download_map.get(source_file) if source_file else None
        url = meta.url if meta else None
        ts_from_downloads = meta.timestamp if meta else None

        # Resolve title candidates for correction detection
        title_candidates = [
            getattr(meta, "title", None) if meta else None,
            item.get("title"),
            item.get("source_title"),
            item.get("document_title"),
        ]
        doc_title = next((t for t in title_candidates if t), "")
        is_corr = is_correction_title(doc_title)

        # --- symbol + insider bypass logic ---
        holder_type_norm = (item.get("holder_type") or "").strip().lower()
        symbol_full = normalize_symbol(item.get("symbol"), item.get("issuer_code"))
        insider_bypass = (holder_type_norm == "insider" and not symbol_full)

        # Company info:
        if insider_bypass:
            company_row = CompanyInfo(
                company_name=item.get("company_name") or item.get("company_name_raw") or ""
            )
        else:
            if not symbol_full:
                logger.debug("Skip item without symbol: %s", _holder_text(item))
                continue
            row = get_company_info(symbol_full)
            company_row = row or CompanyInfo(
                company_name=item.get("company_name") or item.get("company_name_raw") or ""
            )

        company_name = company_row.company_name or item.get("company_name") or item.get("company_name_raw") or ""

        # Prepare correction alert (actual timestamp assigned later)
        corr_alert = None
        if is_corr:
            corr_alert = {
                "reason": "Correction filing in title",
                "title": doc_title,
                "company": company_name,
                "symbol": symbol_full,
                "holder_name": _holder_text(item),
                "UID": item.get("UID"),
                "source": url,
                "timestamp": None,
            }

        txs: List[Dict[str, Any]] = item.get("transactions") or []
        tx_type = (item.get("transaction_type") or "").lower()
        parsed_price = item.get("price")
        parsed_tx_value = item.get("transaction_value")

        # Derive tx_type/amount/price from transactions if missing
        if not tx_type and txs:
            tx_type, net_value, main_txs = determine_transaction_type_from_list(txs)
            parsed_tx_value = parsed_tx_value or net_value

            # === unified with DB field name ===
            amt_from_txs = sum(safe_int(t.get("amount")) for t in main_txs)
            item["amount_transaction"] = amt_from_txs
            parsed_price = average_price(main_txs) if amt_from_txs else 0.0

            # propagate before/after snapshots
            item["holding_before"] = txs[0].get("holding_before", item.get("holding_before"))
            item["holding_after"] = txs[-1].get("holding_after", item.get("holding_after"))
            item["share_percentage_before"] = txs[0].get("share_percentage_before", item.get("share_percentage_before"))
            item["share_percentage_after"] = txs[-1].get("share_percentage_after", item.get("share_percentage_after"))

        hb = safe_int(item.get("holding_before"))
        ha = safe_int(item.get("holding_after"))

        # read amount with backward-compat fallback
        amount_transacted = safe_int(
            item.get("amount_transaction") or item.get("amount_transacted")
        )
        if amount_transacted == 0:
            amount_transacted = abs(ha - hb)

        if not tx_type:
            tx_type = "buy" if ha > hb else ("sell" if ha < hb else "other")

        before_pct = safe_float(item.get("share_percentage_before"))
        after_pct = safe_float(item.get("share_percentage_after"))
        pct_tx = abs(after_pct - before_pct)
        # tags kept for body/text; actual stored tags are recomputed below
        # tags = get_tags(tx_type, before_pct, after_pct)

        # price/value estimation
        latest_price = None if insider_bypass else get_latest_price(symbol_full)
        estimated_value = compute_estimated_value(parsed_price, parsed_tx_value, amount_transacted, latest_price)
        inferred_price = (estimated_value / amount_transacted) if amount_transacted else 0.0

        # timestamps
        ts_primary = item.get("timestamp") or ts_from_downloads
        timestamp_db, nice_date = parse_timestamp(ts_primary)

        if corr_alert is not None:
            corr_alert["timestamp"] = timestamp_db
            correction_alerts.append(corr_alert)

        passes_threshold = (estimated_value > VALUE_KEEP_THRESHOLD) or (pct_tx > PCT_KEEP_THRESHOLD)
        pt_norm = normalize_price_transaction(item, tx_type, txs)

        # mixed transfer
        buy_sell_txs = [t for t in (txs or []) if str(t.get("type") or "").lower() in {"buy", "sell"}]
        transfer_txs = [t for t in (txs or []) if _is_transfer_row(t)]
        is_mixed_transfer = bool(buy_sell_txs and transfer_txs)

        if is_mixed_transfer:
            bs_amt = sum(safe_int(t.get("amount")) for t in buy_sell_txs)
            tr_amt = sum(safe_int(t.get("amount")) for t in transfer_txs)
            bs_val = sum((safe_float(t.get("price")) or 0.0) * safe_int(t.get("amount")) for t in buy_sell_txs)
            tr_val = sum((safe_float(t.get("price")) or 0.0) * safe_int(t.get("amount")) for t in transfer_txs)
            holder_for_alert = _holder_text(item)
            mix_transfer_alerts.append({
                "reason": "mix_transfer",
                "symbol": symbol_full,
                "company": company_name,
                "holder_name": holder_for_alert,
                "transaction_type_declared": tx_type or None,
                "timestamp": timestamp_db,
                "source": url,
                "counts": {"buy_sell": len(buy_sell_txs), "transfer": len(transfer_txs)},
                "amounts": {"buy_sell": bs_amt, "transfer": tr_amt},
                "values": {"buy_sell": bs_val, "transfer": tr_val},
                "transfer_rows": [
                    {"price": safe_float(t.get("price")),
                     "amount": safe_int(t.get("amount")),
                     "date": t.get("date")}
                    for t in transfer_txs
                ],
                "price_transaction": pt_norm,
                "announcement": {"title": doc_title},
            })

        # price deviation alert only when not correction and we HAVE market price
        if (not is_corr) and latest_price is not None and amount_transacted:
            deviation = abs(inferred_price - latest_price) / latest_price if latest_price else 0.0
            if passes_threshold and deviation > PRICE_DEVIATION_THRESHOLD:
                alerts.append({
                    "reason": "Suspicious price deviation (>50%) from latest market price",
                    "holder_name": _holder_text(item),
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
                    # keep both keys for compatibility; DB doesn't read alerts anyway
                    "amount_transaction": amount_transacted,
                    "amount_transacted": amount_transacted,
                    "transaction_value": estimated_value,
                    "price_transaction": pt_norm,
                })

        # UID only if we have symbol
        uid = item.get("UID")
        if (item.get("is_transfer") is True) and not uid and symbol_full:
            uid = maybe_generate_uid(symbol_full, ts_primary, parsed_price, amount_transacted)

        holder = _holder_text(item)
        tx_verb = ({"buy": "bought", "sell": "sold", "transfer": "transferred", "other": "transferred"}
                   .get(tx_type, f"{tx_type}ed")).lower()

        filing = {
            "title": f"{holder} {tx_type.capitalize()} Transaction of {company_name}",
            "body": f"On {nice_date}, {holder}, an {item.get('holder_type')} shareholder, "
                    f"{tx_verb} {amount_transacted:,} shares of {company_name}, changing its holding "
                    f"from {hb:,} to {ha:,} shares.",
            "source": url,
            "timestamp": timestamp_db,
            "sector": slug(company_row.sector or "") if company_row.sector else "",
            "sub_sector": slug(company_row.sub_sector or "") if company_row.sub_sector else "",
            "tags": json.dumps(get_tags(tx_type, before_pct, after_pct)),
            "symbol": symbol_full or "",  # keep empty for insider bypass w/o symbol
            "transaction_type": tx_type or None,
            "holding_before": str(hb),
            "holding_after": str(ha),
            # === DB field name ===
            "amount_transaction": str(amount_transacted),
            "holder_type": item.get("holder_type"),
            "holder_name": holder,
            "price": str(parsed_price if parsed_price is not None else 0.0),
            "transaction_value": str(estimated_value),
            "price_transaction": json.dumps(pt_norm),
            "share_percentage_before": f"{before_pct:.3f}",
            "share_percentage_after": f"{after_pct:.3f}",
            "share_percentage_transaction": f"{pct_tx:.3f}",
            "UID": uid,
        }

        # keep filings only if pass threshold and not mixed transfer
        if passes_threshold and not is_mixed_transfer:
            results.append(filing)
        else:
            if is_mixed_transfer:
                logger.debug("[SKIP filings] mixed transfer detected → only separate alert, no filing")
            else:
                logger.debug("[SKIP] %s → Rp%s, Δ%s%%", holder, f"{estimated_value:,.0f}", f"{pct_tx:.3f}")

    _write_correction_alerts(correction_alerts)
    _write_mix_transfer_alerts(mix_transfer_alerts)

    return results, alerts

