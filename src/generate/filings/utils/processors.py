# src/generate/filings/utils/processors.py
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from statistics import median

from src.core.types import FilingRecord, PriceTransaction

try:
    # Config + external providers (preferred relative imports)
    from .config import (
        WITHIN_DOC_RATIO_LOW,
        WITHIN_DOC_RATIO_HIGH,
        MARKET_REF_N_DAYS,
        MARKET_RATIO_LOW,
        MARKET_RATIO_HIGH,
        ZERO_MISSING_X10_MIN,
        ZERO_MISSING_X10_MAX,
        ZERO_MISSING_X100_MIN,
        ZERO_MISSING_X100_MAX,
        PERCENT_TOL_PP,
        GATE_REASONS,
    )
    from .provider import (
        get_market_reference,
        suggest_price_range,   # kept for future use
        build_announcement_block,
    )
except Exception:
    # Flat layout fallback (kept for compatibility with older runners)
    from config import (
        WITHIN_DOC_RATIO_LOW, WITHIN_DOC_RATIO_HIGH, MARKET_REF_N_DAYS,
        MARKET_RATIO_LOW, MARKET_RATIO_HIGH, ZERO_MISSING_X10_MIN,
        ZERO_MISSING_X10_MAX, ZERO_MISSING_X100_MIN, ZERO_MISSING_X100_MAX,
        PERCENT_TOL_PP, GATE_REASONS,
    )
    from provider import (
        get_market_reference, suggest_price_range, build_announcement_block,
    )

logger = logging.getLogger(__name__)

# Generic helpers
def _safe_float(x: Any) -> Optional[float]:
    """Coerce to float; return None if not parseable/empty."""
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None

def _safe_int(x: Any) -> Optional[int]:
    """Coerce to int; return None if not parseable/empty."""
    try:
        if x is None or x == "":
            return None
        return int(x)
    except Exception:
        return None

def _tx_direction_sign(tx_type: Optional[str]) -> int:
    """
    Map transaction type to a signed direction:
      buy  -> +1
      sell -> -1
      other/transfer/unknown -> 0
    """
    t = (tx_type or "").strip().lower()
    if t == "buy":
        return +1
    if t == "sell":
        return -1
    return 0

def _ratio(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """Compute a/b with guards; return None for invalid inputs."""
    if a is None or b is None:
        return None
    if b == 0:
        return None
    try:
        return float(a) / float(b)
    except Exception:
        return None

# Downloads/meta resolver (to attach announcement links/metadata)
def _basename(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    try:
        return Path(str(s)).name
    except Exception:
        return str(s)

def _stem(s: Optional[str]) -> Optional[str]:
    b = _basename(s)
    if not b:
        return None
    try:
        return Path(b).stem
    except Exception:
        return b

def _pick_first(*vals):
    """Return the first non-empty value among vals."""
    for v in vals:
        if v not in (None, "", [], {}):
            return v
    return None

def _resolve_doc_meta(
    record: FilingRecord,
    downloads_meta_map: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """
    Try to retrieve a metadata block for the document (used to build an
    'announcement' section for alerts):
      1) record.source (often a URL)
      2) basename(source), stem(source)
      3) record.raw_data candidates: source | pdf_url | original_url | url | filename | file
         (each tried as exact, basename, and stem)
    """
    if not downloads_meta_map:
        return None

    candidates: List[str] = []
    candidates.append(_pick_first(getattr(record, "source", None), record.raw_data.get("source")))
    for k in ("pdf_url", "original_url", "url", "filename", "file"):
        v = record.raw_data.get(k)
        if v:
            candidates.append(v)

    # Deduplicate while preserving order
    seen = set()
    keys: List[str] = []
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            keys.append(c)

    # Try exact -> basename -> stem
    for key in keys:
        for probe in (key, _basename(key), _stem(key)):
            if not probe:
                continue
            # Fast path
            if probe in downloads_meta_map:
                it = downloads_meta_map[probe]
                if isinstance(it, dict):
                    return it
            # Loose scan fallback
            for mk, mv in downloads_meta_map.items():
                if mk == probe or _basename(mk) == probe or _stem(mk) == probe:
                    if isinstance(mv, dict):
                        return mv

    return None

# PriceTransaction normalization
# Shape A: List[PriceTransaction]
# Shape B: Single dict with vector fields ({date,type,price,amount_transacted})
# Shape C: List[dict] with "transaction_*" keys (or short aliases)
def _is_db_collapsed_block(obj: Any) -> bool:
    return isinstance(obj, dict) and all(k in obj for k in ("date", "type", "price", "amount_transacted"))

def _expand_db_collapsed_block(block: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Expand a collapsed block (Shape B) into a list of per-transaction dicts."""
    dates = block.get("date", []) or []
    types = block.get("type", []) or []
    prices = block.get("price", []) or []
    amts = block.get("amount_transacted", []) or []

    n = max(len(dates), len(types), len(prices), len(amts))
    out: List[Dict[str, Any]] = []
    for i in range(n):
        out.append({
            "transaction_date": (dates[i] if i < len(dates) else None),
            "transaction_type": (types[i] if i < len(types) else None),
            "transaction_price": (prices[i] if i < len(prices) else None),
            "transaction_share_amount": (amts[i] if i < len(amts) else None),
        })
    return out

def _normalize_price_tx_list(maybe_list: Any) -> List[Any]:
    """
    Normalize to a homogeneous list of per-transaction dicts/objects for local calculations.
    Does not mutate the original record.
    """
    if not isinstance(maybe_list, list) or not maybe_list:
        return []
    first = maybe_list[0]
    if isinstance(first, dict) and len(maybe_list) == 1 and _is_db_collapsed_block(first):
        return _expand_db_collapsed_block(first)
    return maybe_list

def _tx_get_type(tx: Any) -> Optional[str]:
    if isinstance(tx, PriceTransaction):
        return tx.transaction_type
    if isinstance(tx, dict):
        return tx.get("transaction_type") or tx.get("type")
    return None

def _tx_get_price(tx: Any) -> Optional[float]:
    if isinstance(tx, PriceTransaction):
        return tx.transaction_price
    if isinstance(tx, dict):
        return _safe_float(tx.get("transaction_price") or tx.get("price"))
    return None

def _tx_get_amount(tx: Any) -> Optional[int]:
    if isinstance(tx, PriceTransaction):
        return tx.transaction_share_amount
    if isinstance(tx, dict):
        return _safe_int(
            tx.get("transaction_share_amount")
            or tx.get("amount")
            or tx.get("amount_transacted")
        )
    return None

# Inference helpers
def _infer_total_shares(
    holding_before: Optional[float],
    pp_before: Optional[float],
    holding_after: Optional[float],
    pp_after: Optional[float],
) -> Optional[float]:
    """
    Infer total_shares from (holding, percentage), preferring the 'before' pair.
    """
    hb = _safe_float(holding_before)
    pb = _safe_float(pp_before)
    ha = _safe_float(holding_after)
    pa = _safe_float(pp_after)

    if hb is not None and pb is not None and pb > 0:
        return hb / (pb / 100.0)
    if ha is not None and pa is not None and pa > 0:
        return ha / (pa / 100.0)
    return None

def _compute_document_median_price(prices: List[Union[int, float]]) -> Optional[float]:
    vals = [float(x) for x in prices if isinstance(x, (int, float)) and x > 0]
    if not vals:
        return None
    return float(median(vals))

def _is_x10_or_x100(a: Optional[float], ref: Optional[float]) -> Tuple[bool, Optional[str]]:
    """
    Detect potential missing-zero cases (x10/x100) comparing price to a reference.
    """
    r = _ratio(a, ref)
    if r is None:
        return (False, None)
    rr = abs(r)
    if ZERO_MISSING_X10_MIN <= rr <= ZERO_MISSING_X10_MAX:
        return (True, "x10_candidate")
    if ZERO_MISSING_X100_MIN <= rr <= ZERO_MISSING_X100_MAX:
        return (True, "x100_candidate")
    return (False, None)

# P0-4: Suspicious transaction price detection
def _check_tx_price_outlier(
    tx_price: Optional[float],
    doc_median: Optional[float],
    market_ref: Optional[Dict[str, Any]],
) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    Check a single transaction price against:
      1) within-document median
      2) market reference (VWAP/median close over N days)
      3) magnitude anomalies (possible missing zero)
    Returns (suspicious: bool, reasons: List[reason-dicts])
    """
    reasons: List[Dict[str, Any]] = []
    price = _safe_float(tx_price)
    if price is None or price <= 0:
        return (False, reasons)

    suspicious = False

    # 1) Deviation vs document median
    if doc_median:
        r = _ratio(price, doc_median)
        if r is not None and (r < WITHIN_DOC_RATIO_LOW or r > WITHIN_DOC_RATIO_HIGH):
            suspicious = True
            reasons.append({
                "scope": "tx",
                "code": "price_deviation_within_doc",
                "message": f"Price deviates from doc-median by ratio {r:.3f}",
                "details": {"price": price, "doc_median_price": doc_median, "ratio": r},
            })

    # 2) Deviation vs market reference
    market_ref_price = _safe_float((market_ref or {}).get("ref_price"))
    if market_ref_price:
        r = _ratio(price, market_ref_price)
        if r is not None and (r < MARKET_RATIO_LOW or r > MARKET_RATIO_HIGH):
            suspicious = True
            reasons.append({
                "scope": "tx",
                "code": "price_deviation_market",
                "message": f"Price deviates from market-ref by ratio {r:.3f}",
                "details": {"price": price, "market_ref": market_ref, "ratio": r},
            })

        # 3) Magnitude anomaly vs market
        zflag, zlabel = _is_x10_or_x100(price, market_ref_price)
        if zflag:
            suspicious = True
            reasons.append({
                "scope": "tx",
                "code": "possible_zero_missing",
                "message": f"Price magnitude anomaly vs market-ref ({zlabel})",
                "details": {"price": price, "market_ref": market_ref, "ratio": r},
            })

    # If we don't have market data, still check magnitude vs doc median
    elif doc_median:
        zflag, zlabel = _is_x10_or_x100(price, doc_median)
        if zflag:
            suspicious = True
            reasons.append({
                "scope": "tx",
                "code": "possible_zero_missing",
                "message": f"Price magnitude anomaly vs doc-median ({zlabel})",
                "details": {"price": price, "doc_median_price": doc_median, "ratio": _ratio(price, doc_median)},
            })

    return (suspicious, reasons)

# P0-5: Recompute ownership percentages
def _recompute_percentages_model(record: FilingRecord) -> Dict[str, Any]:
    """
    Recompute delta percentage from transactions and compare to PDF-reported
    percentages. Writes results into record.audit_flags.
    """
    share_pct_before_pdf = record.share_percentage_before
    share_pct_after_pdf = record.share_percentage_after
    holding_before = record.holding_before
    holding_after = record.holding_after

    # Prefer parser-provided total_shares when available
    total_shares = _safe_float(record.raw_data.get("total_shares"))
    if total_shares is None:
        total_shares = _infer_total_shares(
            holding_before, share_pct_before_pdf,
            holding_after, share_pct_after_pdf,
        )

    tx_list = _normalize_price_tx_list(record.price_transaction)

    # Net signed amount (+ for buy, − for sell)
    signed_amount = 0.0
    for tx in tx_list:
        amt = _safe_float(_tx_get_amount(tx)) or 0.0
        sgn = _tx_direction_sign(_tx_get_type(tx))
        signed_amount += sgn * amt

    delta_pp_model = None
    pp_after_model = None
    percent_discrepancy = False
    discrepancy_pp = None

    if total_shares and total_shares > 0:
        delta_pp_model = (signed_amount / total_shares) * 100.0
        if share_pct_before_pdf is not None:
            pp_after_model = share_pct_before_pdf + delta_pp_model

    if pp_after_model is not None and share_pct_after_pdf is not None:
        discrepancy_pp = abs(pp_after_model - share_pct_after_pdf)
        if discrepancy_pp > PERCENT_TOL_PP:
            percent_discrepancy = True

    audit = {
        "total_shares_model": total_shares,
        "delta_pp_model": delta_pp_model,
        "pp_after_model": pp_after_model,
        "percent_discrepancy": percent_discrepancy,
        "discrepancy_pp": discrepancy_pp,
    }
    record.audit_flags.update(audit)
    return audit

# Single-record processor
def process_filing_record(
    record: FilingRecord,
    doc_meta: Optional[Union[Dict[str, Any], Any]] = None,
) -> FilingRecord:
    """
    Enrich a FilingRecord with:
      - announcement block (pdf/source links for alerts)
      - document median price
      - market reference snapshot
      - per-transaction suspicious price reasons
      - recomputed % ownership audit
      - gating flags: needs_review, skip_reason
      - reasons list (attached to both audit_flags and record.reasons if present)
    Does NOT modify canonical fields of the record.
    """
    reasons: List[Dict[str, Any]] = []
    row_flags: Dict[str, bool] = {}

    # 1) Announcement block (used by Alerts v2 → adds pdf/source URLs)
    if doc_meta is not None:
        try:
            record.audit_flags["announcement"] = build_announcement_block(doc_meta)
        except Exception as e:
            logger.warning("build_announcement_block failed: %s", e)

    symbol = record.symbol

    # 2) Normalize transactions (supports multiple shapes)
    tx_list = _normalize_price_tx_list(record.price_transaction)

    # 3) Document median price (for within-doc sanity checks)
    tx_prices = [_tx_get_price(tx) for tx in tx_list if _tx_get_price(tx)]
    doc_median_price = _compute_document_median_price(tx_prices)
    if doc_median_price is not None:
        record.audit_flags["document_median_price"] = doc_median_price

    # 4) Market reference window
    market_ref: Optional[Dict[str, Any]] = None
    try:
        market_ref = get_market_reference(symbol, n_days=MARKET_REF_N_DAYS)
    except Exception as e:
        logger.warning("get_market_reference failed for %s: %s", symbol, e)
    if market_ref:
        record.audit_flags["market_reference"] = market_ref

    # 5) Per-transaction suspicious price checks
    any_suspicious = False
    for tx in tx_list:
        suspicious, tx_reasons = _check_tx_price_outlier(
            _tx_get_price(tx), doc_median_price, market_ref
        )
        if tx_reasons:
            reasons.extend(tx_reasons)
        if suspicious:
            any_suspicious = True

    if any_suspicious:
        row_flags["suspicious_price_level"] = True

    # 6) Ownership % recomputation audit
    audit = _recompute_percentages_model(record)
    if audit.get("percent_discrepancy"):
        reasons.append({
            "scope": "row",
            "code": "percent_discrepancy",
            "message": f"Model pp_after deviates from PDF by {audit.get('discrepancy_pp')} pp",
            "details": audit,
        })
        row_flags["percent_discrepancy"] = True

    # 7) Gate + actionable flags
    needs_review = False
    skip_reason: Optional[str] = None

    for r in reasons:
        code = (r.get("code") or "").strip()
        if code in GATE_REASONS:
            needs_review = True
            skip_reason = code
            break

    if not skip_reason and row_flags.get("suspicious_price_level"):
        needs_review = True
        skip_reason = "suspicious_price_level"

    # 8) Persist flags + reasons for downstream consumers (alerts/email)
    record.audit_flags.update(row_flags)
    record.audit_flags["needs_review"] = needs_review
    record.skip_reason = skip_reason

    # Attach reasons both into audit_flags and (if present) top-level .reasons
    record.audit_flags["reasons"] = reasons
    if hasattr(record, "reasons"):
        try:
            record.reasons = reasons  # enables simple consumers to read reasons directly
        except Exception:
            # If FilingRecord is frozen/immutable in your version, we at least keep audit_flags.reasons
            logger.debug("Could not set record.reasons (immutable?). Reasons kept in audit_flags.")

    return record

# Batch processor
def process_all_records(
    records: List[FilingRecord],
    downloads_meta_map: Dict[str, Any] | None = None,
) -> List[FilingRecord]:
    """
    Process a list of FilingRecords:
      - Resolve document metadata from downloads_meta_map (so alerts can embed urls)
      - Run per-record processing and attach reasons/flags
    """
    processed_records: List[FilingRecord] = []
    for rec in records:
        doc_meta = _resolve_doc_meta(rec, downloads_meta_map)
        processed_records.append(process_filing_record(rec, doc_meta=doc_meta))
    return processed_records
