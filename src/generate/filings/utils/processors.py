# src/generate/filings/processors.py
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple, Union

# Local imports (relative)
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
        compute_document_median_price,
        suggest_price_range,
        build_announcement_block,
    )
except Exception:
    # flat layout fallback for test runners
    from config import (
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
    from provider import (
        get_market_reference,
        compute_document_median_price,
        suggest_price_range,
        build_announcement_block,
    )

logger = logging.getLogger(__name__)

# ======================================================================================
# Utilities
# ======================================================================================

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None

def _first_non_empty(*vals):
    for v in vals:
        if v is not None:
            return v
    return None

def _tx_direction_sign(tx_type: Optional[str]) -> int:
    t = (tx_type or "").strip().lower()
    if t == "buy":
        return +1
    if t == "sell":
        return -1
    # transfer/other → 0 (tidak mengubah total outstanding; treat neutral)
    return 0

def _infer_total_shares(
    holding_before: Optional[float],
    pp_before: Optional[float],
    holding_after: Optional[float],
    pp_after: Optional[float],
) -> Optional[float]:
    """
    Infer total_shares dari kombinasi holding + percentage, prioritaskan before.
    """
    hb = _safe_float(holding_before)
    pb = _safe_float(pp_before)
    ha = _safe_float(holding_after)
    pa = _safe_float(pp_after)

    if hb is not None and pb and pb > 0:
        return hb / (pb / 100.0)
    if ha is not None and pa and pa > 0:
        return ha / (pa / 100.0)
    return None

def _median_price_from_transactions(transactions: List[Dict[str, Any]]) -> Optional[float]:
    prices = []
    for tx in transactions or []:
        p = _safe_float(tx.get("price"))
        if p is not None and p > 0:
            prices.append(p)
    return compute_document_median_price(prices)

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
    Deteksi kandidat nol hilang (x10/x100). Kembalikan (flag, label).
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

# ======================================================================================
# P0-4 • Suspicious price detection (within-doc + market sanity + magnitude)
# ======================================================================================

def _check_tx_price_outlier(
    tx: Dict[str, Any],
    doc_median: Optional[float],
    market_ref: Optional[Dict[str, Any]],
) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    Kembalikan:
      suspicious (bool),
      reasons (list of reason objects for this tx)
    """
    reasons: List[Dict[str, Any]] = []
    price = _safe_float(tx.get("price"))
    if price is None or price <= 0:
        return (False, reasons)

    suspicious = False

    # Within-doc deviation
    if doc_median:
        r = _ratio(price, doc_median)
        if r is not None and (r < WITHIN_DOC_RATIO_LOW or r > WITHIN_DOC_RATIO_HIGH):
            suspicious = True
            reasons.append({
                "scope": "tx",
                "code": "price_deviation_within_doc",
                "message": f"Price deviates vs doc-median by ratio {r:.3f} "
                           f"(thresholds {WITHIN_DOC_RATIO_LOW}-{WITHIN_DOC_RATIO_HIGH})",
                "details": {
                    "price": price,
                    "doc_median_price": doc_median,
                    "ratio": r,
                    "suggest_price_range": suggest_price_range(doc_median),
                },
            })

    # Market sanity vs N-day VWAP/median-close
    if market_ref and "ref_price" in market_ref:
        ref_p = _safe_float(market_ref.get("ref_price"))
        r = _ratio(price, ref_p)
        if r is not None and (r < MARKET_RATIO_LOW or r > MARKET_RATIO_HIGH):
            suspicious = True
            reasons.append({
                "scope": "tx",
                "code": "price_deviation_market",
                "message": f"Price deviates vs market-ref by ratio {r:.3f} "
                           f"(thresholds {MARKET_RATIO_LOW}-{MARKET_RATIO_HIGH})",
                "details": {
                    "price": price,
                    "market_ref": market_ref,
                    "ratio": r,
                    "suggest_price_range": suggest_price_range(ref_p),
                },
            })

        # Magnitude anomaly (possible zero missing)
        zflag, zlabel = _is_x10_or_x100(price, ref_p)
        if zflag:
            suspicious = True
            reasons.append({
                "scope": "tx",
                "code": "possible_zero_missing",
                "message": f"Price magnitude anomaly vs market-ref ({zlabel})",
                "details": {
                    "price": price,
                    "market_ref": market_ref,
                    "ratio": r,
                    "magnitude_label": zlabel,
                },
            })

    # Magnitude anomaly vs doc median as alternate reference if market missing
    elif doc_median:
        zflag, zlabel = _is_x10_or_x100(price, doc_median)
        if zflag:
            suspicious = True
            reasons.append({
                "scope": "tx",
                "code": "possible_zero_missing",
                "message": f"Price magnitude anomaly vs doc-median ({zlabel})",
                "details": {
                    "price": price,
                    "doc_median_price": doc_median,
                    "ratio": _ratio(price, doc_median),
                    "magnitude_label": zlabel,
                },
            })

    return (suspicious, reasons)

# ======================================================================================
# P0-5 • Recompute ownership % (model vs PDF → flag only)
# ======================================================================================

def _recompute_percentages_model(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Hitung pp_after_model dan delta_pp_model berbasis total_shares inferred.
    Tidak override nilai PDF; hanya menambahkan field audit:
      - total_shares_model
      - delta_pp_model  (pp transact total)
      - pp_after_model
      - percent_discrepancy (bool)
    """
    # Ambil basis "before"
    share_pct_before_pdf = _safe_float(_first_non_empty(
        row.get("share_percentage_before"),
        row.get("pp_before"),
    ))
    share_pct_after_pdf = _safe_float(_first_non_empty(
        row.get("share_percentage_after"),
        row.get("pp_after"),
    ))

    holding_before = _safe_float(row.get("holding_before"))
    holding_after = _safe_float(row.get("holding_after"))
    total_shares = _safe_float(row.get("total_shares"))  # kalau sudah tersedia

    if total_shares is None:
        total_shares = _infer_total_shares(
            holding_before, share_pct_before_pdf,
            holding_after, share_pct_after_pdf,
        )

    # Sum semua transaksi (buy + / sell -)
    txs: List[Dict[str, Any]] = row.get("transactions") or []
    signed_amount = 0.0
    for tx in txs:
        amt = _safe_float(tx.get("amount")) or 0.0
        sgn = _tx_direction_sign(tx.get("type"))
        signed_amount += sgn * amt

    delta_pp_model = None
    pp_after_model = None
    percent_discrepancy = False
    discrepancy_pp = None

    if total_shares and total_shares > 0:
        delta_pp_model = (signed_amount / total_shares) * 100.0
        if share_pct_before_pdf is not None:
            pp_after_model = share_pct_before_pdf + delta_pp_model

    # Bandingkan pp_after_model vs pp_after_pdf (kalau keduanya ada)
    if pp_after_model is not None and share_pct_after_pdf is not None:
        discrepancy_pp = abs(pp_after_model - share_pct_after_pdf)
        if discrepancy_pp > PERCENT_TOL_PP:
            percent_discrepancy = True

    # Tambahkan hasil audit ke row
    audit = {
        "total_shares_model": total_shares,
        "delta_pp_model": delta_pp_model,
        "pp_after_model": pp_after_model,
        "percent_discrepancy": percent_discrepancy,
        "discrepancy_pp": discrepancy_pp,
    }
    row.update(audit)
    return audit

# ======================================================================================
# Processor entry (single row)
# ======================================================================================

def process_filing_row(
    row: Dict[str, Any],
    doc_meta: Optional[Union[Dict[str, Any], Any]] = None,
) -> Dict[str, Any]:
    """
    Memproses satu row filings:
      - Suspicious price detection per-transaksi
      - Recompute % (model vs PDF) → flag percent_discrepancy (tidak override)
      - Kumpulkan reasons[] (row & tx)
      - Set needs_review, suspicious_price_level, skip_reason (untuk gating di mailer)
      - Tambah announcement block (untuk Alerts v2)
      - Tambah doc_median & market_ref untuk konteks alert
    Return: row (mutated) dengan field tambahan
    """
    # Persiapan struktur
    row.setdefault("reasons", [])
    reasons: List[Dict[str, Any]] = row["reasons"]
    row_flags: Dict[str, bool] = {}

    # Announcement block (Alerts v2)
    if doc_meta is not None:
        row["announcement"] = build_announcement_block(doc_meta)

    # Context symbol & prices
    symbol = row.get("symbol") or row.get("issuer_code") or row.get("ticker")
    symbol = (symbol or "").upper()
    if symbol and not symbol.endswith(".JK"):
        symbol = f"{symbol}.JK"

    # Doc-median price
    txs: List[Dict[str, Any]] = row.get("transactions") or []
    doc_median_price = _median_price_from_transactions(txs)
    if doc_median_price is not None:
        row["document_median_price"] = doc_median_price

    # Market reference (N-day)
    market_ref = get_market_reference(symbol, n_days=MARKET_REF_N_DAYS)
    if market_ref:
        row["market_reference"] = market_ref

    # -------- P0-4: Suspicious price detection per transaksi --------
    any_suspicious = False
    for tx in txs:
        suspicious, tx_reasons = _check_tx_price_outlier(tx, doc_median_price, market_ref)
        if tx_reasons:
            # Tambahkan reasons ke level transaksi
            tx.setdefault("reasons", [])
            tx["reasons"].extend(tx_reasons)
            # Promosikan juga ke level row untuk Alerts v2 (dengan penanda scope=tx)
            reasons.extend(tx_reasons)
        if suspicious:
            any_suspicious = True
            tx["suspicious_price_level"] = True

    if any_suspicious:
        row_flags["suspicious_price_level"] = True

    # -------- P0-5: Recompute % kepemilikan (model vs PDF) --------
    audit = _recompute_percentages_model(row)
    if audit.get("percent_discrepancy"):
        reasons.append({
            "scope": "row",
            "code": "percent_discrepancy",
            "message": (
                f"Model pp_after={audit.get('pp_after_model')} deviates from PDF "
                f"pp_after={row.get('share_percentage_after')} by {audit.get('discrepancy_pp')} pp "
                f"(> {PERCENT_TOL_PP} pp)"
            ),
            "details": {
                "pp_after_pdf": row.get("share_percentage_after"),
                "pp_after_model": audit.get("pp_after_model"),
                "delta_pp_model": audit.get("delta_pp_model"),
                "total_shares_model": audit.get("total_shares_model"),
                "tolerance_pp": PERCENT_TOL_PP,
            },
        })
        row_flags["percent_discrepancy"] = True

    # -------- Needs review + skip_reason (for gating in send_alerts.py) --------
    needs_review = False
    skip_reason: Optional[str] = None

    # Urutan prioritas skip_reason: ambil reason pertama yang termasuk GATE_REASONS
    for r in reasons:
        code = (r.get("code") or "").strip()
        if code in GATE_REASONS:
            needs_review = True
            skip_reason = code
            break

    # Jika tidak ada reasons-ber-gate namun ada suspicious_price_level
    if not skip_reason and row_flags.get("suspicious_price_level"):
        needs_review = True
        skip_reason = "suspicious_price_level"

    # Set summary flags
    row["needs_review"] = bool(needs_review)
    row["skip_reason"] = skip_reason
    row.update(row_flags)

    return row

# ======================================================================================
# Convenience: process a document with multiple rows
# ======================================================================================

def process_document_rows(
    rows: List[Dict[str, Any]],
    doc_meta: Optional[Union[Dict[str, Any], Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Helper untuk memproses banyak row dari satu dokumen.
    """
    out = []
    for r in rows or []:
        out.append(process_filing_row(r, doc_meta=doc_meta))
    return out

# ======================================================================================
# Adapter: process_all (untuk kompatibilitas pipeline lama)
# ======================================================================================

def process_all(
    parsed_lists: List[List[Dict[str, Any]]],
    downloads_meta_map: Dict[str, Any] | None = None,  # tidak dipakai di adaptor ini
) -> List[Dict[str, Any]]:
    """
    Minimal adapter: flatten parsed chunks lalu kirim ke processor baru.
    CATATAN:
    - Jika pipeline lamamu biasanya membentuk row via builder khusus di utils.processors,
      lebih baik pakai kembali builder itu, lalu panggil process_document_rows(rows, doc_meta).
      Adaptor ini hanya mem-forward apa adanya agar tidak error import.
    """
    flat_rows: List[Dict[str, Any]] = []
    for chunk in (parsed_lists or []):
        for r in (chunk or []):
            if isinstance(r, dict):
                flat_rows.append(r)
    # doc_meta tidak tersedia di level ini → kirim None
    return process_document_rows(flat_rows, doc_meta=None)

__all__ = [
    "process_filing_row",
    "process_document_rows",
    "process_all",
]
