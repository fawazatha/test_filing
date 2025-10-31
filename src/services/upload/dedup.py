# src/services/upload/dedup.py
from __future__ import annotations

import asyncio
import json
import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx

# Import the uploader class
try:
    from .supabase import SupabaseUploader
except ImportError:
    from services.upload.supabase import SupabaseUploader 

# Import the fetcher from the scripts directory
try:
    from scripts.fetch_filings import get_idx_filings_by_days
except ImportError:
    # Fallback for different execution contexts
    logging.warning("Could not import 'get_idx_filings_by_days' from scripts.fetch_filings")
    # Define a dummy function to avoid crashing
    async def get_idx_filings_by_days(*args, **kwargs) -> List[Dict[str, Any]]:
        logging.error("Using dummy get_idx_filings_by_days. Deduplication against DB will not be complete.")
        return []


# ---------- normalizers ----------
def _to_day(s: Optional[str]) -> str:
    """Converts an ISO string to YYYY-MM-DD."""
    if not s:
        return ""
    try:
        return datetime.fromisoformat(s).date().isoformat()
    except Exception:
        return str(s)[:10]  # "YYYY-MM-DD" best-effort

def _norm_float(v: Any, ndigits: int = 6):
    """Normalize float for hashing."""
    if v is None or v == "":
        return None
    try:
        return round(float(str(v).replace(",", "").strip()), ndigits)
    except Exception:
        return None

# ---------- hashing (local) ----------
def make_filing_hash(row: Dict[str, Any]) -> str:
    """
    Creates a SHA-256 hash from the key fields of a filing.
    This row should be a dict, e.g., from FilingRecord.to_db_dict().
    """
    # Use 'filing_date' if present, otherwise fall back to 'timestamp'
    # This key matches the field used in get_idx_filings_by_days
    filing_date_key = _to_day(row.get("filing_date") or row.get("timestamp"))

    key_data = {
        "symbol": (row.get("symbol") or "").strip().upper(),
        "filing_date": filing_date_key,
        "type": (row.get("transaction_type") or row.get("type") or "").strip().lower(),
        "holder_name": (row.get("holder_name") or "").strip().lower(),
        "holding_before": row.get("holding_before"),
        "holding_after": row.get("holding_after"),
        "share_pct_before": _norm_float(row.get("share_percentage_before")),
        "share_pct_after": _norm_float(row.get("share_percentage_after")),
        "amount": row.get("amount_transaction"),
        "price": _norm_float(row.get("price")),
    }
    # Normalize None to "" for consistent hashing
    normalized = {k: ("" if v is None else v) for k, v in key_data.items()}
    blob = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _prepare_batch_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Ensure each row has a 'filing_date' (YYYY-MM-DD) for DB query."""
    out = []
    for r in rows:
        rr = dict(r)
        # Use 'filing_date' if it exists, else 'timestamp'
        day = _to_day(rr.get("filing_date") or rr.get("timestamp"))
        if day:
            rr["filing_date"] = day
        out.append(rr)
    return out


def _intrarun_unique(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove duplicates *within* the current batch."""
    seen = set()
    out = []
    for r in rows:
        h = make_filing_hash(r)
        if h in seen:
            continue
        seen.add(h)
        out.append(r)
    return out


# ---------- fetch existing rows from Supabase ----------
def _fetch_existing_rows_same_days(
    uploader: SupabaseUploader,
    table: str,
    days: List[str],
    symbols: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetches rows from Supabase for the specific days and symbols.
    This now calls the async function using asyncio.run().
    """
    if not days:
        return []
        
    try:
        # We must run the async function from our synchronous context
        return asyncio.run(get_idx_filings_by_days(
            days=days,
            symbols=symbols,
            table=table
        ))
    except Exception as e:
        logging.error(f"Failed to fetch existing rows from Supabase: {e}")
        return []


def _db_row_to_hashable(db_row: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure keys from DB match what make_filing_hash expects."""
    out = dict(db_row)
    # Map columns to ensure hash matches
    if "amount" not in out:
        out["amount"] = db_row.get("amount_transaction")
    if "value" not in out:
        out["value"] = db_row.get("transaction_value")
    return out


# ---------- public entry ----------
def upload_filings_with_dedup(
    *,
    uploader: SupabaseUploader,
    table: str,
    rows: List[Dict[str, Any]],
    allowed_columns: Optional[List[str] | set[str]] = None,
    stop_on_first_error: bool = False,
) -> Tuple[UploadResult, Dict[str, int]]:
    """
    Uploads filings to Supabase, first deduplicating against
    existing records from the same day(s).
    """

    # 1. Prepare rows (ensure 'filing_date' is set)
    prepared = _prepare_batch_rows(rows)
    
    # 2. Dedupe within this batch
    intra = _intrarun_unique(prepared)

    # 3. Get days and symbols to check against the DB
    days = sorted({r.get("filing_date") for r in intra if r.get("filing_date")})
    symbols = [r.get("symbol") for r in intra if r.get("symbol")] or None
    
    # 4. Fetch existing rows from the DB for those days
    existing_rows = _fetch_existing_rows_same_days(uploader, table, days, symbols)

    # 5. Create a set of hashes from existing DB rows
    existing_hashes = set()
    for dr in existing_rows:
        existing_hashes.add(make_filing_hash(_db_row_to_hashable(dr)))

    # 6. Filter out any rows that already exist in the DB
    final_rows = []
    for r in intra:
        h = make_filing_hash(r)
        if h not in existing_hashes:
            final_rows.append(r)

    # 7. Upload only the new, unique rows
    res = uploader.upload_records(
        table=table,
        rows=final_rows,                 
        allowed_columns=allowed_columns, 
        stop_on_first_error=stop_on_first_error,
    )

    stats = {
        "input": len(rows),
        "intrarun_unique": len(intra),
        "existing_same_day_rows": len(existing_rows),
        "to_insert": len(final_rows),
        "inserted": getattr(res, "inserted", 0),
        "failed": len(getattr(res, "failed_rows", [])),
    }
    return res, stats