from __future__ import annotations

import argparse
import csv
import json
import os
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


# -------------------------
# Parsing helpers

def _to_str(x: Any) -> str:
    return "" if x is None else str(x)

def _to_date(x: Any) -> str:
    s = _to_str(x).strip()
    return s[:10] if s else ""

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

    # Thousands separators
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


def _parse_json_maybe(s: Any) -> Any:
    if s in (None, "", []):
        return None
    if isinstance(s, (list, dict)):
        return s
    if isinstance(s, str):
        try:
            return json.loads(s)
        except Exception:
            return None
    return None


def _parse_dt(ts: str) -> Optional[datetime]:
    """
    Try to parse timestamp variants:
      - 'YYYY-MM-DD HH:MM:SS'
      - 'YYYY-MM-DDTHH:MM:SS'
      - with timezone suffix '+07:00' or 'Z'
    """
    if not ts:
        return None
    s = ts.strip()
    s = s.replace("T", " ")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _ts_candidates(ts: str) -> List[str]:
    """
    Generate candidate keys for matching, to handle small formatting differences.
    We always include:
      - raw trimmed
      - first 19 chars normalized to 'YYYY-MM-DD HH:MM:SS' if possible
      - UTC-normalized string if tz-aware
    """
    raw = _to_str(ts).strip()
    cands = []
    if raw:
        cands.append(raw)

    dt = _parse_dt(raw)
    if dt:
        # local wall-clock (naive)
        if dt.tzinfo is not None:
            # keep wall-clock string by dropping tzinfo WITHOUT converting
            naive = dt.replace(tzinfo=None)
            cands.append(naive.strftime("%Y-%m-%d %H:%M:%S"))

            # also add UTC instant
            utc_dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            cands.append(utc_dt.strftime("%Y-%m-%d %H:%M:%S"))
        else:
            cands.append(dt.strftime("%Y-%m-%d %H:%M:%S"))

    # also try first 19 chars + normalize 'T'
    if raw:
        s19 = raw.replace("T", " ")[:19]
        cands.append(s19)

    # unique keep order
    seen = set()
    out = []
    for x in cands:
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out


def _canonicalize_list(txs: List[Dict[str, Any]], default_date: str = "", default_type: str = "") -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for t in txs:
        if not isinstance(t, dict):
            continue
        out.append(
            {
                "date": _to_date(t.get("date") or t.get("transaction_date") or default_date),
                "type": _to_str(t.get("type") or default_type).strip(),
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
                    "type": (types[i] if i < len(types) else fallback_type) or fallback_type,
                    "price": prices[i] if i < len(prices) else None,
                    "amount_transacted": amounts[i] if i < len(amounts) else None,
                }
            )
        return _canonicalize_list(items, default_date=fallback_date, default_type=fallback_type)

    return []


def multiset_values(txs: List[Dict[str, Any]], include_date: bool) -> Counter:
    c: Counter = Counter()
    for t in txs:
        typ = _to_str(t.get("type")).strip().lower()
        price = _to_int(t.get("price"))
        amt = _to_int(t.get("amount_transacted"))
        if include_date:
            key = (_to_date(t.get("date")), typ, price, amt)
        else:
            key = (typ, price, amt)
        c[key] += 1
    return c


def counter_to_json(c: Counter) -> str:
    items = []
    for k, cnt in sorted(c.items(), key=lambda x: (x[0], x[1])):
        if isinstance(k, tuple) and len(k) == 3:
            typ, price, amt = k
            items.append({"type": typ, "price": price, "amount_transacted": amt, "count": cnt})
        elif isinstance(k, tuple) and len(k) == 4:
            d, typ, price, amt = k
            items.append({"date": d, "type": typ, "price": price, "amount_transacted": amt, "count": cnt})
        else:
            items.append({"key": str(k), "count": cnt})
    return json.dumps(items, ensure_ascii=False, separators=(",", ":"))


# -------------------------
# CSV IO + grouping

def load_csv(path: str) -> List[Dict[str, Any]]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def make_key(ts: str, sym: str, match: str) -> Tuple[str, str]:
    ts_key = _to_str(ts).strip()
    sym_key = _to_str(sym).strip().upper() if match == "timestamp_symbol" else ""
    return (ts_key, sym_key)


def group_by_keys(rows: List[Dict[str, Any]], match: str, ts_col: str, sym_col: str) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    """
    Group rows by (timestamp[, symbol]).
    We also add "shadow keys" using timestamp candidates to reduce not_found due to formatting.
    """
    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for r in rows:
        ts = r.get(ts_col) or ""
        sym = r.get(sym_col) or ""

        for cand in _ts_candidates(ts):
            k = make_key(cand, sym, match)
            groups.setdefault(k, []).append(r)
    return groups


# -------------------------
# Main

def main() -> None:
    ap = argparse.ArgumentParser(description="Compare preview CSV vs idx_filings export CSV by TIMESTAMP (offline).")
    ap.add_argument("--preview-csv", required=True, help="CSV from preview script (has timestamp + proposed/existing columns)")
    ap.add_argument("--export-csv", required=True, help="Full export of idx_filings table (has timestamp,symbol,price_transaction)")
    ap.add_argument("--mode", choices=["existing", "proposed"], default="proposed",
                    help="Compare export.price_transaction vs preview.existing or preview.proposed (default: proposed)")
    ap.add_argument("--match", choices=["timestamp", "timestamp_symbol"], default="timestamp_symbol",
                    help="Matching key. default: timestamp_symbol (safer). use timestamp if you insist.")
    ap.add_argument("--include-date", action="store_true",
                    help="If set, compare WITH date. Default compare ignores date.")
    ap.add_argument("--out-csv", default="src/scripts/price_transaction_normalizer/data/preview_vs_export_mismatch_by_timestamp.csv",
                    help="Output CSV (mismatches only)")
    ap.add_argument("--preview-ts-col", default="timestamp", help="Preview timestamp column name")
    ap.add_argument("--preview-sym-col", default="symbol_db", help="Preview symbol column name (fallback: symbol_csv if empty)")
    ap.add_argument("--export-ts-col", default="timestamp", help="Export timestamp column name")
    ap.add_argument("--export-sym-col", default="symbol", help="Export symbol column name")
    ap.add_argument("--export-pt-col", default="price_transaction", help="Export price_transaction column name")
    ap.add_argument("--export-type-col", default="transaction_type", help="Export transaction_type column name (optional)")
    args = ap.parse_args()

    preview = load_csv(args.preview_csv)
    export = load_csv(args.export_csv)

    # pick preview column
    preview_col = "proposed_price_transaction" if args.mode == "proposed" else "existing_price_transaction"

    # group export rows by timestamp key(s)
    export_groups = group_by_keys(export, args.match, args.export_ts_col, args.export_sym_col)

    mismatches: List[Dict[str, Any]] = []
    stats = Counter()

    for p in preview:
        ts = p.get(args.preview_ts_col) or ""
        # symbol from preview: prefer symbol_db, fallback symbol_csv
        sym = p.get(args.preview_sym_col) or p.get("symbol_csv") or ""

        # find matching export group by trying candidates
        match_rows: List[Dict[str, Any]] = []
        matched_key: Optional[Tuple[str, str]] = None
        for cand in _ts_candidates(ts):
            k = make_key(cand, sym, args.match)
            if k in export_groups:
                match_rows = export_groups[k]
                matched_key = k
                break

        if not match_rows:
            stats["not_found_in_export"] += 1
            mismatches.append(
                {
                    "timestamp": ts,
                    "symbol": sym,
                    "status": "not_found_in_export",
                    "preview_rows": 1,
                    "export_rows": 0,
                    "preview_counter": "",
                    "export_counter": "",
                    "missing_in_export": "",
                    "extra_in_export": "",
                    "matched_key_ts": "",
                }
            )
            continue

        # Build preview multiset (aggregate)
        # preview_col is already canonical JSON list string (from your preview script)
        preview_items = normalize_price_tx(
            p.get(preview_col),
            fallback_date=_to_date(ts),
            fallback_type=_to_str(p.get("transaction_type")).strip(),
        )
        c_prev = multiset_values(preview_items, include_date=args.include_date)

        # Build export multiset (aggregate across all matching rows)
        export_items_all: List[Dict[str, Any]] = []
        for r in match_rows:
            fallback_type = _to_str(r.get(args.export_type_col)).strip()
            fallback_date = _to_date(r.get(args.export_ts_col))
            export_items_all.extend(
                normalize_price_tx(
                    r.get(args.export_pt_col),
                    fallback_date=fallback_date,
                    fallback_type=fallback_type,
                )
            )
        c_exp = multiset_values(export_items_all, include_date=args.include_date)

        if c_prev == c_exp:
            stats["match"] += 1
            continue  # user wants only mismatches

        stats["mismatch"] += 1
        missing = c_prev - c_exp
        extra = c_exp - c_prev

        mismatches.append(
            {
                "timestamp": ts,
                "symbol": sym,
                "status": "mismatch",
                "preview_rows": 1,
                "export_rows": len(match_rows),
                "preview_counter": counter_to_json(c_prev) if c_prev else "",
                "export_counter": counter_to_json(c_exp) if c_exp else "",
                "missing_in_export": counter_to_json(missing) if missing else "",
                "extra_in_export": counter_to_json(extra) if extra else "",
                "matched_key_ts": matched_key[0] if matched_key else "",
            }
        )

    os.makedirs(os.path.dirname(args.out_csv) or ".", exist_ok=True)
    with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "timestamp",
                "symbol",
                "status",
                "preview_rows",
                "export_rows",
                "matched_key_ts",
                "preview_counter",
                "export_counter",
                "missing_in_export",
                "extra_in_export",
            ],
        )
        w.writeheader()
        w.writerows(mismatches)

    print(f"Wrote mismatches only: {args.out_csv} (rows={len(mismatches)})")
    print("Summary:", dict(stats))


if __name__ == "__main__":
    main()
