from __future__ import annotations

"""
Preview proposed `price_transaction` updates for idx_filings from a CSV.

Reads a CSV with columns:
  id, timestamp, price_transaction, symbol
and fetches the matching rows from Supabase (by id when provided; else by exact timestamp).
It normalizes both existing and proposed price_transaction into a canonical list
of {date, type, price, amount_transacted} and emits a preview CSV (no DB writes)
highlighting differences.
"""

import argparse
import asyncio
import csv
import json
import logging
import os
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import httpx
from dotenv import load_dotenv

# Reuse Supabase REST helpers from fetch_filings
from src.scripts.fetch_filings import _sb_base, _sb_headers, _build_query_params

load_dotenv()

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger("preview_price_tx_update")


# -----------------------------------------------------------------------------
# Helpers

def _parse_price_tx(raw: Any) -> List[Dict[str, Any]]:
    """
    Normalize price_transaction into list[{date,type,price,amount_transacted}].
    Accepts:
      - list of dicts
      - dict with parallel arrays: {"type": [...], "prices": [...], "amount_transacted": [...]}
      - JSON string of either form
    """
    if raw in (None, "", []):
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return []

    out: List[Dict[str, Any]] = []

    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            out.append(
                {
                    "date": (item.get("date") or item.get("transaction_date") or "")[:10],
                    "type": item.get("type"),
                    "price": item.get("price"),
                    "amount_transacted": item.get("amount_transacted") or item.get("amount"),
                }
            )
        return out

    if isinstance(raw, dict):
        types = raw.get("type") or []
        prices = raw.get("prices") or []
        amounts = raw.get("amount_transacted") or []
        max_len = max(len(types), len(prices), len(amounts)) if any([types, prices, amounts]) else 0
        for i in range(max_len):
            out.append(
                {
                    "date": None,  # legacy format had no per-row date
                    "type": types[i] if i < len(types) else None,
                    "price": prices[i] if i < len(prices) else None,
                    "amount_transacted": amounts[i] if i < len(amounts) else None,
                }
            )
        return out

    return []


def _normalized_json(txs: List[Dict[str, Any]]) -> str:
    """Stable JSON string for comparison/output."""
    return json.dumps(txs, ensure_ascii=False, sort_keys=True)


def _status(existing: List[Dict[str, Any]], proposed: List[Dict[str, Any]]) -> str:
    if existing == proposed:
        return "same"
    if not existing and proposed:
        return "missing_in_db"
    if existing and not proposed:
        return "missing_in_csv"
    return "diff"


# -----------------------------------------------------------------------------
# Supabase fetch

async def _rest_get(table: str, qs: Sequence[Tuple[str, str]], *, timeout: float = 30.0) -> List[Dict[str, Any]]:
    url = f"{_sb_base()}/rest/v1/{table}?{httpx.QueryParams(qs)}"
    headers = _sb_headers()
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()
        return r.json()


async def fetch_rows(
    table: str,
    *,
    ids: Sequence[int],
    timestamps: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    """
    Fetch rows by ids (preferred) and by timestamps (exact match) for fallback.
    Returns a dict keyed by id (str) and timestamp (str) for easy lookup.
    """
    out: Dict[str, Dict[str, Any]] = {}

    if ids:
        qs = _build_query_params(
            select="id,timestamp,symbol,price_transaction",
            in_={"id": ids},
        )
        try:
            rows = await _rest_get(table, qs)
            for row in rows:
                if "id" in row:
                    out[str(row["id"])] = row
        except Exception as exc:
            logger.error("Failed fetching by ids: %s", exc, exc_info=True)

    # Fetch timestamps not already covered by ids
    ts_need = [t for t in timestamps if t and t not in out]
    for ts in ts_need:
        qs = _build_query_params(
            select="id,timestamp,symbol,price_transaction",
            eq={"timestamp": ts},
        )
        qs.append(("limit", "1"))
        try:
            rows = await _rest_get(table, qs)
            if rows:
                out[ts] = rows[0]
        except Exception as exc:
            logger.error("Failed fetching timestamp=%s: %s", ts, exc, exc_info=True)

    return out


# -----------------------------------------------------------------------------
# CLI

def load_csv(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    try_encodings = [("utf-8", "strict"), ("utf-8", "replace"), ("latin-1", "strict")]
    last_err = None

    # Pre-sanitize to strip NULs
    with open(path, "rb") as bf:
        raw_bytes = bf.read().replace(b"\x00", b"")

    # Detect obvious non-CSV (e.g., ZIP/Numbers/Excel package)
    if raw_bytes.startswith(b"PK\x03\x04"):
        raise ValueError(
            f"{path} looks like a ZIP/Numbers/Excel package, not a CSV. "
            "Please export to a plain CSV first."
        )

    for enc, err_mode in try_encodings:
        try:
            text = raw_bytes.decode(enc, errors=err_mode)
            r = csv.DictReader(text.splitlines())
            for row in r:
                rows.append(row)
            break
        except Exception as exc:
            rows = []
            last_err = exc
            continue
    if rows == [] and last_err:
        raise last_err
    return rows


async def main(args: argparse.Namespace) -> None:
    inp = load_csv(args.csv)
    ids = []
    for r in inp:
        raw_id = r.get("id")
        if raw_id in (None, ""):
            continue
        try:
            ids.append(int(raw_id))
        except Exception:
            logger.warning("Skipping invalid id=%r", raw_id)
    timestamps = [r.get("timestamp") for r in inp if r.get("timestamp")]

    db_rows = await fetch_rows(args.table, ids=ids, timestamps=timestamps)

    out_rows: List[Dict[str, Any]] = []

    for row in inp:
        rid = row.get("id")
        ts = row.get("timestamp")
        key = str(rid) if rid and str(rid) in db_rows else ts
        db_row = db_rows.get(key)

        existing_tx = _parse_price_tx(db_row.get("price_transaction")) if db_row else []
        proposed_tx = _parse_price_tx(row.get("price_transaction"))

        out_rows.append(
            {
                "id": rid or (db_row.get("id") if db_row else ""),
                "timestamp": ts or (db_row.get("timestamp") if db_row else ""),
                "symbol_csv": row.get("symbol"),
                "symbol_db": db_row.get("symbol") if db_row else "",
                "status": _status(existing_tx, proposed_tx) if db_row else "not_found",
                "existing_price_transaction": _normalized_json(existing_tx) if existing_tx else "",
                "proposed_price_transaction": _normalized_json(proposed_tx) if proposed_tx else "",
            }
        )

    # Write preview CSV
    fieldnames = [
        "id",
        "timestamp",
        "symbol_csv",
        "symbol_db",
        "status",
        "existing_price_transaction",
        "proposed_price_transaction",
    ]
    with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(out_rows)

    logger.info("Wrote preview CSV with %d rows to %s", len(out_rows), args.out_csv)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Preview idx_filings price_transaction updates from CSV (no DB writes).")
    p.add_argument("--csv", required=True, help="Input CSV with columns: id,timestamp,price_transaction,symbol")
    p.add_argument("--table", default="idx_filings", help="Supabase table name (default: idx_filings)")
    p.add_argument("--out-csv", dest="out_csv", default="data/price_tx_preview.csv", help="Preview output CSV path")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    asyncio.run(main(args))
