# src/services/send_alerts.py
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    # Preferred relative imports (monorepo style)
    from src.config.config import (
        GATE_REASONS,
        ALERTS_OUTPUT_DIR,
        ALERTS_INSERTED_FILENAME,
        ALERTS_NOT_INSERTED_FILENAME,
        SUGGEST_PRICE_RATIO,
    )
except Exception:
    # Flat layout fallback for tests
    from src.config.config import (
        GATE_REASONS,
        ALERTS_OUTPUT_DIR,
        ALERTS_INSERTED_FILENAME,
        ALERTS_NOT_INSERTED_FILENAME,
        SUGGEST_PRICE_RATIO,
    )

logger = logging.getLogger(__name__)


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _reason_codes_from_row(row: Dict[str, Any]) -> List[str]:
    codes: List[str] = []
    for r in row.get("reasons") or []:
        c = (r.get("code") or "").strip()
        if c:
            codes.append(c)
    return codes


def is_gated(row: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Tentukan apakah baris ini harus DICORET dari email 'Inserted'.
    Urutan prioritas:
      1) Jika row.needs_review True -> gated, skip_reason = row.skip_reason atau reason pertama yang ada di GATE_REASONS
      2) Jika ada reason.code ⊂ GATE_REASONS -> gated
      3) Jika ada flag umum (fallback): suspicious_price_level/percent_discrepancy/stale_price/missing_price/delta_pp_mismatch -> gated
    """
    if row.get("needs_review") is True:
        sr = (row.get("skip_reason") or "").strip() or None
        if not sr:
            # cari reason yang masuk gate
            for c in _reason_codes_from_row(row):
                if c in GATE_REASONS:
                    sr = c
                    break
        # fallback ke suspicious_price_level kalau belum ada
        if not sr and row.get("suspicious_price_level"):
            sr = "suspicious_price_level"
        return True, sr

    # explicit reasons
    for c in _reason_codes_from_row(row):
        if c in GATE_REASONS:
            return True, c

    # common flags (fallback)
    for k in ("suspicious_price_level", "percent_discrepancy", "stale_price", "missing_price", "delta_pp_mismatch"):
        if row.get(k):
            return True, k

    return False, None


def build_alert_entry(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalisasi payload alert yang ramah untuk email renderer:
    - field inti (symbol, holder, type, amount, price)
    - announcement block (jika ada)
    - reasons[] disalurkan apa adanya (tapi dipastikan list of dict)
    - price suggestions:
        - prefer from document_median_price
        - else from market_reference.ref_price
    """
    symbol = row.get("symbol") or row.get("issuer_code")
    tx_type = (row.get("type") or "").lower() or row.get("tx_type")
    holder = row.get("holder_name") or row.get("shareholder") or ""
    amount = _safe_float(row.get("amount"))
    price = _safe_float(row.get("price"))
    value = _safe_float(row.get("value"))

    # suggestions
    suggest_ref = None
    if _safe_float(row.get("document_median_price")):
        suggest_ref = float(row["document_median_price"])
    elif isinstance(row.get("market_reference"), dict) and _safe_float(row["market_reference"].get("ref_price")):
        suggest_ref = float(row["market_reference"]["ref_price"])
    suggest = None
    if suggest_ref is not None and suggest_ref >= 0:
        delta = suggest_ref * float(SUGGEST_PRICE_RATIO)
        suggest = {
            "ref": suggest_ref,
            "min": max(0.0, suggest_ref - delta),
            "max": suggest_ref + delta,
        }

    # prepare reasons (safe)
    reasons = []
    for r in row.get("reasons") or []:
        if isinstance(r, dict):
            reasons.append({
                "scope": r.get("scope"),
                "code": r.get("code"),
                "message": r.get("message"),
                "details": r.get("details"),
            })

    # audit fields ringkas
    audit = {
        "delta_pp_model": _safe_float(row.get("delta_pp_model")),
        "pp_after_model": _safe_float(row.get("pp_after_model")),
        "percent_discrepancy": bool(row.get("percent_discrepancy") or False),
    }

    entry = {
        "symbol": symbol,
        "type": tx_type,
        "holder_name": holder,
        "amount": amount,
        "price": price,
        "value": value,
        "transaction_date": row.get("transaction_date"),
        "tags": row.get("tags") or [],
        "needs_review": bool(row.get("needs_review") or False),
        "skip_reason": row.get("skip_reason"),
        "reasons": reasons,
        "announcement": row.get("announcement"),
        "document_median_price": _safe_float(row.get("document_median_price")),
        "market_reference": row.get("market_reference"),
        "suggest_price": suggest,
        "audit": audit,
    }
    return entry


def split_alerts(alerts: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Bagi alerts (list row filing) menjadi (inserted, not_inserted) berdasarkan is_gated().
    Return:
      (alerts_inserted, alerts_not_inserted)
    """
    inserted: List[Dict[str, Any]] = []
    not_inserted: List[Dict[str, Any]] = []

    for row in alerts or []:
        gated, reason = is_gated(row)
        entry = build_alert_entry(row)
        # sinkronkan flag
        entry["needs_review"] = gated or bool(row.get("needs_review"))
        if gated and reason and not entry.get("skip_reason"):
            entry["skip_reason"] = reason

        if gated:
            not_inserted.append(entry)
        else:
            inserted.append(entry)

    return inserted, not_inserted


# Optional helpers untuk menulis file alerts_* sesuai pola config
# (boleh diabaikan jika kamu sudah pakai services.email.bucketize)
def _resolve_filename(pattern: str, date_str: str) -> str:
    try:
        return pattern.format(date=date_str)
    except Exception:
        # fallback
        return f"{pattern.rstrip('.json')}_{date_str}.json"


def write_alert_files(
    *,
    alerts_rows: List[Dict[str, Any]],
    date_str: str,
    out_dir: Optional[str] = None,
    inserted_pattern: Optional[str] = None,
    not_inserted_pattern: Optional[str] = None,
) -> Tuple[Path, Path]:
    """
    Tulis dua file JSON:
      - alerts_inserted_{date}.json
      - alerts_not_inserted_{date}.json
    berdasarkan hasil split_alerts(alerts_rows).

    Mengembalikan tuple (inserted_path, not_inserted_path).
    """
    out_root = Path(out_dir or ALERTS_OUTPUT_DIR)
    out_root.mkdir(parents=True, exist_ok=True)

    ins_pat = inserted_pattern or ALERTS_INSERTED_FILENAME
    not_pat = not_inserted_pattern or ALERTS_NOT_INSERTED_FILENAME

    ins_name = _resolve_filename(ins_pat, date_str)
    not_name = _resolve_filename(not_pat, date_str)

    inserted, not_inserted = split_alerts(alerts_rows)

    ins_path = out_root / ins_name
    not_path = out_root / not_name

    ins_path.write_text(json.dumps(inserted, ensure_ascii=False, indent=2), encoding="utf-8")
    not_path.write_text(json.dumps(not_inserted, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("[ALERTS] wrote %s (inserted=%d)", ins_path, len(inserted))
    logger.info("[ALERTS] wrote %s (not_inserted=%d)", not_path, len(not_inserted))

    return ins_path, not_path


__all__ = [
    "is_gated",
    "build_alert_entry",
    "split_alerts",
    "write_alert_files",
]
