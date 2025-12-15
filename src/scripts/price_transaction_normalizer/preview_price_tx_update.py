from __future__ import annotations

"""
Offline preview: add `date` to DB-export `price_transaction` using a source CSV.

- DB export CSV: scripts/price_transaction_normalizer/data/idx_filings_rows.csv (truth for type/price/amount_transacted)
- Source CSV:    data/input.csv (truth for date; schema already list[{date,type,price,amount_transacted}] ideally)

Matching:
- by timestamp (or timestamp+symbol)

Validation:
- Compare DB vs source by multiset of (type, price, amount_transacted), ignoring date
- Types are canonicalized so transfer/neutral/others -> other

Output:
- out-preview: all rows with status + proposed JSON
- out-mismatch: only rows with problems
"""

import argparse
import csv
import json
import os
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


# Basic utilities
def _to_str(x: Any) -> str:
    return "" if x is None else str(x)

def _to_date(x: Any) -> str:
    s = _to_str(x).strip()
    return s[:10] if s else ""

def canon_type(t: Any) -> str:
    """
    Canonicalize transaction types.
    - transfer/neutral/others/other => other
    - buy/sell => buy/sell
    """
    s = _to_str(t).strip().lower()
    if s in ("transfer", "neutral", "others", "other"):
        return "other"
    return s

def _to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    if isinstance(x, bool):
        return int(x)
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        if x != x:  # NaN
            return None
        return int(x)

    s = _to_str(x).strip()
    if not s:
        return None
    s = s.replace(" ", "")

    # Handle thousands separators and decimals robustly
    if "," in s and "." in s:
        s = s.replace(",", "")
        try:
            return int(float(s))
        except Exception:
            return None

    if "," in s and "." not in s:
        s = s.replace(",", "")
        try:
            return int(float(s))
        except Exception:
            return None

    if "." in s and "," not in s:
        parts = s.split(".")
        # treat as thousands separator if groups like 1.234.567
        if len(parts) > 1 and all(len(p) == 3 for p in parts[1:]) and parts[0].isdigit():
            try:
                return int("".join(parts))
            except Exception:
                return None
        try:
            return int(float(s))
        except Exception:
            return None

    try:
        return int(float(s))
    except Exception:
        return None

def _parse_json_maybe(x: Any) -> Any:
    if x in (None, "", []):
        return None
    if isinstance(x, (list, dict)):
        return x
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            return None
    return None


# Timestamp normalization (reduce not-found due to formatting/timezone)

def _parse_dt(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    s = ts.strip().replace("T", " ")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _ts_candidates(ts: str) -> List[str]:
    """
    Generate multiple representations so timestamps match even if formats differ.
    """
    raw = _to_str(ts).strip()
    cands: List[str] = []
    if raw:
        cands.append(raw)

    dt = _parse_dt(raw)
    if dt:
        if dt.tzinfo is not None:
            # wall-clock without tz
            naive = dt.replace(tzinfo=None)
            cands.append(naive.strftime("%Y-%m-%d %H:%M:%S"))

            # UTC instant
            utc_dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            cands.append(utc_dt.strftime("%Y-%m-%d %H:%M:%S"))
        else:
            cands.append(dt.strftime("%Y-%m-%d %H:%M:%S"))

    if raw:
        cands.append(raw.replace("T", " ")[:19])

    seen = set()
    out: List[str] = []
    for x in cands:
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out


# price_transaction normalization (v1/v2/v3/target)

def _canonicalize_list(
    txs: List[Dict[str, Any]],
    default_date: str = "",
    default_type: str = "",
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for t in txs:
        if not isinstance(t, dict):
            continue
        out.append(
            {
                "date": _to_date(t.get("date") or t.get("transaction_date") or default_date),
                "type": canon_type(t.get("type") or default_type),
                "price": _to_int(t.get("price")),
                "amount_transacted": _to_int(
                    t.get("amount_transacted") if t.get("amount_transacted") is not None else t.get("amount")
                ),
            }
        )
    return out

def normalize_price_tx(raw: Any, fallback_date: str = "", fallback_type: str = "") -> List[Dict[str, Any]]:
    """
    Normalize price_transaction into list[{date,type,price,amount_transacted}].
    Covers:
      - target list
      - legacy v1 {"prices":[...],"amount_transacted":[...]}
      - legacy v2 {"types":[...],"prices":[...],"amount_transacted":[...]}
      - legacy v3 {"type":[...],"prices":[...],"amount_transacted":[...]}
    """
    raw = _parse_json_maybe(raw)
    if raw in (None, "", []):
        return []

    if isinstance(raw, list):
        return _canonicalize_list(raw, default_date=fallback_date, default_type=fallback_type)

    if isinstance(raw, dict):
        prices = raw.get("prices") or []
        if not isinstance(prices, list):
            prices = [prices]

        amounts = raw.get("amount_transacted")
        if amounts is None:
            amounts = raw.get("amount")
        amounts = amounts or []
        if not isinstance(amounts, list):
            amounts = [amounts]

        types = raw.get("types")
        if types is None:
            types = raw.get("type")
        if types is None:
            types = []
        if not isinstance(types, list):
            types = [types]

        max_len = max(len(prices), len(amounts), len(types)) if any([prices, amounts, types]) else 0
        items: List[Dict[str, Any]] = []
        for i in range(max_len):
            items.append(
                {
                    "date": fallback_date,
                    "type": canon_type((types[i] if i < len(types) else fallback_type) or fallback_type),
                    "price": prices[i] if i < len(prices) else None,
                    "amount_transacted": amounts[i] if i < len(amounts) else None,
                }
            )
        return _canonicalize_list(items, default_date=fallback_date, default_type=fallback_type)

    return []

def _multiset_no_date(txs: List[Dict[str, Any]]) -> Counter:
    """
    Compare by (type, price, amount_transacted), ignoring date.
    Type is canonicalized.
    """
    c: Counter = Counter()
    for t in txs:
        key = (
            canon_type(t.get("type")),
            _to_int(t.get("price")),
            _to_int(t.get("amount_transacted")),
        )
        c[key] += 1
    return c

def _json_stable(x: Any) -> str:
    return json.dumps(x, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


# Transfer date from source to DB (only add date)
def apply_dates_from_source(
    db_items: List[Dict[str, Any]],
    src_items: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Precondition: values match ignoring date.
    Strategy: FIFO assign dates per (type, price, amount_transacted) bucket.
    """
    buckets: Dict[Tuple[str, Optional[int], Optional[int]], deque] = defaultdict(deque)

    for s in src_items:
        k = (
            canon_type(s.get("type")),
            _to_int(s.get("price")),
            _to_int(s.get("amount_transacted")),
        )
        buckets[k].append(_to_date(s.get("date")))

    proposed: List[Dict[str, Any]] = []
    filled = 0

    for d in db_items:
        k = (
            canon_type(d.get("type")),
            _to_int(d.get("price")),
            _to_int(d.get("amount_transacted")),
        )

        cur_date = _to_date(d.get("date"))
        if not cur_date and buckets[k]:
            new_date = buckets[k].popleft()
            if new_date:
                cur_date = new_date
                filled += 1

        proposed.append(
            {
                "date": cur_date,
                "type": canon_type(d.get("type")),
                "price": _to_int(d.get("price")),
                "amount_transacted": _to_int(d.get("amount_transacted")),
            }
        )

    return proposed, filled


# CSV IO + indexing
def load_csv(path: str) -> List[Dict[str, Any]]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def make_key(ts: str, sym: str, match: str) -> Tuple[str, str]:
    ts_key = _to_str(ts).strip()
    sym_key = _to_str(sym).strip().upper() if match == "timestamp_symbol" else ""
    return (ts_key, sym_key)

def index_source(rows: List[Dict[str, Any]], ts_col: str, sym_col: str, match: str) -> Dict[Tuple[str, str], Dict[str, Any]]:
    idx: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in rows:
        ts = r.get(ts_col) or ""
        sym = r.get(sym_col) or ""
        for cand in _ts_candidates(ts):
            idx[make_key(cand, sym, match)] = r
    return idx


# Main
def main() -> None:
    ap = argparse.ArgumentParser(description="Offline preview: add date to DB-export price_transaction using source CSV.")
    ap.add_argument("--db-csv", required=True, help="DB export CSV (e.g. src/scripts/price_transaction_normalizer/data/idx_filings_rows.csv)")
    ap.add_argument("--source-csv", required=True, help="Source CSV (correct schema with date)")
    ap.add_argument("--match", choices=["timestamp", "timestamp_symbol"], default="timestamp_symbol")

    ap.add_argument("--out-preview", default="src/scripts/price_transaction_normalizer/data/price_tx_preview_from_export.csv")
    ap.add_argument("--out-mismatch", default="src/scripts/price_transaction_normalizer/data/price_tx_preview_from_export_mismatch_only.csv")

    ap.add_argument("--db-ts-col", default="timestamp")
    ap.add_argument("--db-sym-col", default="symbol")
    ap.add_argument("--db-type-col", default="transaction_type")
    ap.add_argument("--db-pt-col", default="price_transaction")

    ap.add_argument("--src-ts-col", default="timestamp")
    ap.add_argument("--src-sym-col", default="symbol")
    ap.add_argument("--src-pt-col", default="price_transaction")

    args = ap.parse_args()

    db_rows = load_csv(args.db_csv)
    src_rows = load_csv(args.source_csv)
    src_idx = index_source(src_rows, args.src_ts_col, args.src_sym_col, args.match)

    out_all: List[Dict[str, Any]] = []
    out_bad: List[Dict[str, Any]] = []

    stats = Counter()

    for db in db_rows:
        ts = db.get(args.db_ts_col) or ""
        sym = db.get(args.db_sym_col) or ""

        # Find matching source row by timestamp candidates
        src = None
        used_ts = ""
        for cand in _ts_candidates(ts):
            k = make_key(cand, sym, args.match)
            if k in src_idx:
                src = src_idx[k]
                used_ts = cand
                break

        fallback_date = _to_date(ts)
        fallback_type = canon_type(db.get(args.db-type-col) if False else db.get(args.db_type_col))
        existing_items = normalize_price_tx(
            db.get(args.db_pt_col),
            fallback_date=fallback_date,
            fallback_type=fallback_type,
        )

        if not src:
            stats["not_found_in_source"] += 1
            row = {
                "timestamp": ts,
                "symbol": sym,
                "status": "not_found_in_source",
                "matched_ts_key": "",
                "filled_date_count": 0,
                "existing_price_transaction": _json_stable(existing_items) if existing_items else "",
                "source_price_transaction": "",
                "proposed_price_transaction": "",
                "missing_in_db": "",
                "extra_in_db": "",
            }
            out_all.append(row)
            out_bad.append(row)
            continue

        src_items = normalize_price_tx(
            src.get(args.src_pt_col),
            fallback_date=_to_date(src.get(args.src_ts_col) or ts),
            fallback_type="",  # source should already include per-item type
        )

        c_db = _multiset_no_date(existing_items)
        c_src = _multiset_no_date(src_items)

        if c_db != c_src:
            stats["values_mismatch"] += 1
            missing = c_src - c_db
            extra = c_db - c_src
            row = {
                "timestamp": ts,
                "symbol": sym,
                "status": "values_mismatch",
                "matched_ts_key": used_ts,
                "filled_date_count": 0,
                "existing_price_transaction": _json_stable(existing_items) if existing_items else "",
                "source_price_transaction": _json_stable(src_items) if src_items else "",
                "proposed_price_transaction": "",
                "missing_in_db": _json_stable(
                    [{"type": k[0], "price": k[1], "amount_transacted": k[2], "count": v} for k, v in missing.items()]
                ) if missing else "",
                "extra_in_db": _json_stable(
                    [{"type": k[0], "price": k[1], "amount_transacted": k[2], "count": v} for k, v in extra.items()]
                ) if extra else "",
            }
            out_all.append(row)
            out_bad.append(row)
            continue

        proposed_items, filled = apply_dates_from_source(existing_items, src_items)
        stats["ok"] += 1

        row = {
            "timestamp": ts,
            "symbol": sym,
            "status": "ok",
            "matched_ts_key": used_ts,
            "filled_date_count": filled,
            "existing_price_transaction": _json_stable(existing_items) if existing_items else "",
            "source_price_transaction": _json_stable(src_items) if src_items else "",
            "proposed_price_transaction": _json_stable(proposed_items) if proposed_items else "",
            "missing_in_db": "",
            "extra_in_db": "",
        }
        out_all.append(row)

    os.makedirs(os.path.dirname(args.out_preview) or ".", exist_ok=True)
    preview_fields = [
        "timestamp",
        "symbol",
        "status",
        "matched_ts_key",
        "filled_date_count",
        "existing_price_transaction",
        "source_price_transaction",
        "proposed_price_transaction",
        "missing_in_db",
        "extra_in_db",
    ]
    with open(args.out_preview, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=preview_fields, extrasaction="ignore")
        w.writeheader()
        for r in out_all:
            w.writerow({k: r.get(k, "") for k in preview_fields})

    os.makedirs(os.path.dirname(args.out_mismatch) or ".", exist_ok=True)
    mismatch_fields = [
        "timestamp",
        "symbol",
        "status",
        "matched_ts_key",
        "existing_price_transaction",
        "source_price_transaction",
        "missing_in_db",
        "extra_in_db",
    ]
    with open(args.out_mismatch, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=mismatch_fields, extrasaction="ignore")
        w.writeheader()
        for r in out_bad:
            w.writerow({k: r.get(k, "") for k in mismatch_fields})

    print("Summary:", dict(stats))
    print("Wrote preview:", args.out_preview)
    print("Wrote mismatch-only:", args.out_mismatch)


if __name__ == "__main__":
    main()
