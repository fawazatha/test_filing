# src/generate/filings/utils/processors.py
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple, Union
from statistics import median

# Import the new standard type
from src.core.types import FilingRecord, PriceTransaction

# Keep all your config and provider imports
try:
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
        # compute_document_median_price, # We'll redefine this locally
        suggest_price_range,
        build_announcement_block,
    )
except Exception:
    # flat layout fallback
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

# Utilities
def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None

def _tx_direction_sign(tx_type: Optional[str]) -> int:
    t = (tx_type or "").strip().lower()
    if t == "buy":
        return +1
    if t == "sell":
        return -1
    # transfer/other -> 0
    return 0

def _infer_total_shares(
    holding_before: Optional[float],
    pp_before: Optional[float],
    holding_after: Optional[float],
    pp_after: Optional[float],
) -> Optional[float]:
    """
    Infer total_shares from holding + percentage, prioritizing 'before'.
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
    """Helper to compute median from a list of prices."""
    vals = [float(x) for x in prices if isinstance(x, (int, float)) and x > 0]
    if not vals:
        return None
    return float(median(vals))


def _ratio(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    if b == 0:
        return None
    try:
        return float(a) / float(b)
    except Exception:
        return None

def _is_x10_or_x100(a: Optional[float], ref: Optional[float]) -> Tuple[bool, Optional[str]]:
    """
    Detects potential missing zero candidates (x10/x100).
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


# P0-4 • Suspicious price detection
def _check_tx_price_outlier(
    tx_price: Optional[float],
    doc_median: Optional[float],
    market_ref: Optional[Dict[str, Any]],
) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    Checks a single transaction price for deviations.
    Returns: suspicious (bool), reasons (list)
    """
    reasons: List[Dict[str, Any]] = []
    price = _safe_float(tx_price)
    if price is None or price <= 0:
        return (False, reasons)

    suspicious = False

    # 1. Within-doc deviation
    if doc_median:
        r = _ratio(price, doc_median)
        if r is not None and (r < WITHIN_DOC_RATIO_LOW or r > WITHIN_DOC_RATIO_HIGH):
            suspicious = True
            reasons.append({
                "scope": "tx",
                "code": "price_deviation_within_doc",
                "message": f"Price deviates vs doc-median by ratio {r:.3f}",
                "details": { "price": price, "doc_median_price": doc_median, "ratio": r },
            })

    # 2. Market sanity vs N-day VWAP/median-close
    market_ref_price = _safe_float((market_ref or {}).get("ref_price"))
    if market_ref_price:
        r = _ratio(price, market_ref_price)
        if r is not None and (r < MARKET_RATIO_LOW or r > MARKET_RATIO_HIGH):
            suspicious = True
            reasons.append({
                "scope": "tx",
                "code": "price_deviation_market",
                "message": f"Price deviates vs market-ref by ratio {r:.3f}",
                "details": { "price": price, "market_ref": market_ref, "ratio": r },
            })

        # 3. Magnitude anomaly (possible zero missing)
        zflag, zlabel = _is_x10_or_x100(price, market_ref_price)
        if zflag:
            suspicious = True
            reasons.append({
                "scope": "tx",
                "code": "possible_zero_missing",
                "message": f"Price magnitude anomaly vs market-ref ({zlabel})",
                "details": { "price": price, "market_ref": market_ref, "ratio": r },
            })

    # 4. Magnitude anomaly vs doc median (if market is missing)
    elif doc_median:
        zflag, zlabel = _is_x10_or_x100(price, doc_median)
        if zflag:
            suspicious = True
            reasons.append({
                "scope": "tx",
                "code": "possible_zero_missing",
                "message": f"Price magnitude anomaly vs doc-median ({zlabel})",
                "details": { "price": price, "doc_median_price": doc_median, "ratio": _ratio(price, doc_median) },
            })

    return (suspicious, reasons)


# P0-5 • Recompute ownership %
def _recompute_percentages_model(record: FilingRecord) -> Dict[str, Any]:
    """
    Calculates ownership percentages based on transactions and compares
    to the PDF values. Writes results to record.audit_flags.
    """
    # Get clean data from the record
    share_pct_before_pdf = record.share_percentage_before
    share_pct_after_pdf = record.share_percentage_after
    holding_before = record.holding_before
    holding_after = record.holding_after
    
    # Check raw data for total_shares if provided by parser
    total_shares = _safe_float(record.raw_data.get("total_shares"))

    if total_shares is None:
        total_shares = _infer_total_shares(
            holding_before, share_pct_before_pdf,
            holding_after, share_pct_after_pdf,
        )

    # Sum all transactions (buy + / sell -)
    signed_amount = 0.0
    for tx in record.price_transaction:
        amt = _safe_float(tx.transaction_share_amount) or 0.0
        sgn = _tx_direction_sign(tx.transaction_type)
        signed_amount += sgn * amt

    delta_pp_model = None
    pp_after_model = None
    percent_discrepancy = False
    discrepancy_pp = None

    if total_shares and total_shares > 0:
        delta_pp_model = (signed_amount / total_shares) * 100.0
        if share_pct_before_pdf is not None:
            pp_after_model = share_pct_before_pdf + delta_pp_model

    # Compare model's 'after' vs PDF's 'after'
    if pp_after_model is not None and share_pct_after_pdf is not None:
        discrepancy_pp = abs(pp_after_model - share_pct_after_pdf)
        if discrepancy_pp > PERCENT_TOL_PP:
            percent_discrepancy = True

    # Add audit results to the record's audit_flags
    audit = {
        "total_shares_model": total_shares,
        "delta_pp_model": delta_pp_model,
        "pp_after_model": pp_after_model,
        "percent_discrepancy": percent_discrepancy,
        "discrepancy_pp": discrepancy_pp,
    }
    record.audit_flags.update(audit)
    return audit


# Processor entry (single record)
def process_filing_record(
    record: FilingRecord,
    doc_meta: Optional[Union[Dict[str, Any], Any]] = None,
) -> FilingRecord:
    """
    This function "enriches" a FilingRecord with audit flags
    and checks. It does NOT transform data.
    """
    reasons: List[Dict[str, Any]] = []
    row_flags: Dict[str, bool] = {} # e.g., suspicious_price_level

    # 1. Announcement block (for Alerts v2)
    if doc_meta is not None:
        record.audit_flags["announcement"] = build_announcement_block(doc_meta)

    # 2. Get symbol (already clean)
    symbol = record.symbol

    # 3. Doc-median price
    tx_prices = [tx.transaction_price for tx in record.price_transaction if tx.transaction_price]
    doc_median_price = _compute_document_median_price(tx_prices)
    if doc_median_price is not None:
        record.audit_flags["document_median_price"] = doc_median_price

    # 4. Market reference (N-day)
    market_ref = get_market_reference(symbol, n_days=MARKET_REF_N_DAYS)
    if market_ref:
        record.audit_flags["market_reference"] = market_ref

    # 5. P0-4: Suspicious price detection per transaction
    any_suspicious = False
    for tx in record.price_transaction:
        suspicious, tx_reasons = _check_tx_price_outlier(
            tx.transaction_price, doc_median_price, market_ref
        )
        if tx_reasons:
            reasons.extend(tx_reasons)
        if suspicious:
            any_suspicious = True

    if any_suspicious:
        row_flags["suspicious_price_level"] = True

    # 6. P0-5: Recompute % kepemilikan (model vs PDF)
    audit = _recompute_percentages_model(record)
    if audit.get("percent_discrepancy"):
        reasons.append({
            "scope": "row",
            "code": "percent_discrepancy",
            "message": f"Model pp_after deviates from PDF by {audit.get('discrepancy_pp')} pp",
            "details": audit,
        })
        row_flags["percent_discrepancy"] = True

    # 7. Set 'needs_review' and 'skip_reason' for alerts
    needs_review = False
    skip_reason: Optional[str] = None

    # Find the first reason code that is in GATE_REASONS
    for r in reasons:
        code = (r.get("code") or "").strip()
        if code in GATE_REASONS:
            needs_review = True
            skip_reason = code
            break
            
    if not skip_reason and row_flags.get("suspicious_price_level"):
        needs_review = True
        skip_reason = "suspicious_price_level"

    # Set final flags
    record.audit_flags["needs_review"] = needs_review
    record.audit_flags.update(row_flags)
    record.skip_reason = skip_reason # This is used by alerts

    return record


# Main Processor 
def process_all_records(
    records: List[FilingRecord],
    downloads_meta_map: Dict[str, Any] | None = None,
) -> List[FilingRecord]:
    """
    New adapter that processes a list of FilingRecord objects.
    
    (Future improvement: group records by document_id and
    pass 'doc_meta' to process_filing_record)
    """
    processed_records = []
    for rec in records:
        # TODO: Find the doc_meta from downloads_meta_map if needed
        doc_meta = None
        processed_records.append(process_filing_record(rec, doc_meta=doc_meta))
    return processed_records