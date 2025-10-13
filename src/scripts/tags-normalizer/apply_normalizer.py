#!/usr/bin/env python3
import os, csv, sys, json, argparse, time, math
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv, find_dotenv
from supabase import create_client, Client

TABLE = "idx_filings"
PK = "id"
COL_BEFORE = "share_percentage_before"
COL_AFTER  = "share_percentage_after"

# -----------------------
# 9-tag policy components
# -----------------------
SENTIMENTS = {"bullish", "bearish"}
REASON_TAGS = {
    "divestment",
    "investment",
    "free-float-requirement",
    "MESOP",
    "inheritance",
    "share-transfer",
}
CONTROL_TAGS = {"takeover"}
WHITELIST = SENTIMENTS | REASON_TAGS | CONTROL_TAGS
EXPLICIT_DROP = {
    "insider-trading", "insider trading",
    "ownership-change", "ownership change",
}

VARIANTS = {
    # reasons
    "mesop": "MESOP",
    "free-float-requirement": "free-float-requirement",
    "freefloatrequirement": "free-float-requirement",
    "free-float": "free-float-requirement",
    "freefloat": "free-float-requirement",
    "share-transfer": "share-transfer",
    "sharetransfer": "share-transfer",
    "share transfer": "share-transfer",
    "investment": "investment",
    "invest": "investment",
    "divestment": "divestment",
    "divest": "divestment",
    "inheritance": "inheritance",
    "inheritence": "inheritance",
    # sentiment
    "bullish": "bullish",
    "bull": "bullish",
    "bearish": "bearish",
    "bear": "bearish",
    # control
    "takeover": "takeover",
    "take-over": "takeover",
}

def hyphenize(s: str) -> str:
    import re
    s = re.sub(r"[_\s]+", "-", s.strip())
    s = re.sub(r"-{2,}", "-", s)
    return s

def canonicalize_one(tag: str) -> Optional[str]:
    k = hyphenize(str(tag)).lower()
    if k in EXPLICIT_DROP:
        return None
    if k == "mesop":
        return "MESOP"
    mapped = VARIANTS.get(k)
    if mapped is None:
        return None
    return "MESOP" if mapped.lower() == "mesop" else mapped

def keep_only_reason_tags(raw_tags: Any) -> List[str]:
    """From CSV-provided tags keep only the 6 'reason' tags (normalized)."""
    if raw_tags is None:
        arr: List[str] = []
    elif isinstance(raw_tags, list):
        arr = [str(x) for x in raw_tags]
    else:
        arr = [str(raw_tags)]
    mapped = [canonicalize_one(t) for t in arr]
    return [t for t in mapped if t in REASON_TAGS]

def derive_sentiment(before: Any, after: Any) -> Optional[str]:
    """bullish if after>before, bearish if after<before, else None."""
    try:
        b = float(before) if before is not None else None
        a = float(after)  if after  is not None else None
    except Exception:
        return None
    if b is None or a is None:
        return None
    if a > b:
        return "bullish"
    if a < b:
        return "bearish"
    return None

def takeover_cross_both_directions(before: Any, after: Any) -> bool:
    """
    Control crossing in either direction:
    - upward:   before < 50 and after >= 50
    - downward: before >= 50 and after < 50
    """
    try:
        b = float(before) if before is not None else None
        a = float(after)  if after  is not None else None
    except Exception:
        return False
    if b is None or a is None:
        return False
    return (b < 50.0 and a >= 50.0) or (b >= 50.0 and a < 50.0)

def assemble_new_tags(csv_tags: Any, before: Any, after: Any) -> List[str]:
    # 1) keep only reasons from CSV
    out = keep_only_reason_tags(csv_tags)
    # 2) add recomputed sentiment
    s = derive_sentiment(before, after)
    if s:
        out.append(s)
    # 3) add takeover if control crossing (both directions per latest rule)
    if takeover_cross_both_directions(before, after):
        out.append("takeover")

    # 4) dedupe + filter + sort (preserve MESOP uppercase)
    seen = set()
    uniq: List[str] = []
    for t in out:
        if t in WHITELIST and t not in seen:
            uniq.append(t); seen.add(t)
    uniq.sort(key=lambda x: (x != "MESOP", x.lower()))
    return uniq

def normalize_old_for_compare(old_tags: Any) -> List[str]:
    """Normalize any old tags to canonical forms to detect real changes."""
    if isinstance(old_tags, list):
        arr = [str(x) for x in old_tags]
    elif old_tags is None:
        arr = []
    else:
        arr = [str(old_tags)]
    mapped = [canonicalize_one(t) for t in arr]
    mapped = [t for t in mapped if t in WHITELIST]
    mapped = sorted(set(mapped), key=lambda x: (x != "MESOP", x.lower()))
    return mapped

def parse_tags_cell(cell: str):
    if cell is None:
        return []
    s = cell.strip()
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return arr
        except Exception:
            pass
    if "|" in s and "," not in s:
        return [x.strip() for x in s.split("|") if x.strip()]
    return [x.strip() for x in s.split(",") if x.strip()]

def fetch_percentages_for_ids(supabase: Client, ids: List[str|int], batch_size: int = 1000) -> Dict[str, Dict[str, Any]]:
    """Return map id -> { before, after } fetched from DB."""
    out: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(ids), batch_size):
        chunk = ids[i:i+batch_size]
        resp = supabase.table(TABLE).select(f"{PK},{COL_BEFORE},{COL_AFTER}").in_(PK, chunk).execute()
        rows = resp.data or []
        for r in rows:
            out[str(r[PK])] = { "before": r.get(COL_BEFORE), "after": r.get(COL_AFTER) }
    return out

def main():
    ap = argparse.ArgumentParser(description="Apply CSV tags with normalization + DB-derived sentiment/takeover")
    ap.add_argument("--table", default=TABLE)
    ap.add_argument("--pk", default=PK)
    ap.add_argument("--in", dest="csv_in", required=True, help="CSV with columns: id,tags")
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--report", help="Write would-be updates to this JSONL (dry-run friendly)")
    ap.add_argument("--print", dest="print_n", type=int, default=0, help="Print first N updates to console")
    args = ap.parse_args()

    load_dotenv(find_dotenv(usecwd=True))
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        print("ERROR: Missing SUPABASE_URL or SUPABASE_KEY in .env", file=sys.stderr)
        sys.exit(1)
    supabase: Client = create_client(url, key)

    # 1) read CSV
    csv_rows: List[Dict[str, Any]] = []
    with open(args.csv_in, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        if args.pk not in r.fieldnames or "tags" not in r.fieldnames:
            print(f"ERROR: CSV must have columns: {args.pk}, tags", file=sys.stderr); sys.exit(1)
        for row in r:
            pk = row[args.pk]
            tags = parse_tags_cell(row.get("tags", "[]"))
            if isinstance(tags, str):
                try:
                    tags = json.loads(tags)
                    if not isinstance(tags, list):
                        tags = [tags]
                except Exception:
                    tags = [tags]
            csv_rows.append({args.pk: pk, "tags": tags})

    ids = [row[args.pk] for row in csv_rows]
    print(f"[info] loaded {len(ids)} rows from CSV")

    # 2) fetch percentages from DB for those IDs
    pct_map = fetch_percentages_for_ids(supabase, ids)
    missing = [i for i in ids if str(i) not in pct_map]
    if missing:
        print(f"[warn] {len(missing)} ids not found in DB (will still upsert tags from CSV reasons + no sentiment/takeover)")

    # 3) build normalized updates
    updates: List[Dict[str, Any]] = []
    for row in csv_rows:
        pk = row[args.pk]
        p = pct_map.get(str(pk), {})
        before = p.get("before")
        after  = p.get("after")
        new_tags = assemble_new_tags(row["tags"], before, after)
        updates.append({args.pk: pk, "tags": new_tags})

    print(f"[info] prepared {len(updates)} normalized updates (dry_run={args.dry_run})")

    # 4) report / preview
    if args.report:
        with open(args.report, "w", encoding="utf-8") as out:
            for u in updates:
                out.write(json.dumps(u, ensure_ascii=False) + "\n")
        print(f"[report] wrote JSONL → {args.report}")

    if args.print_n and args.print_n > 0:
        print(f"[preview] first {min(args.print_n, len(updates))} updates:")
        for u in updates[:args.print_n]:
            print(u)

    # 5) apply (unless dry-run)
    if args.dry_run:
        print("[dry-run] not writing to Supabase")
        return

    total = 0
    for i in range(0, len(updates), args.batch):
        chunk = updates[i:i+args.batch]
        resp = supabase.table(args.table).upsert(chunk, on_conflict=args.pk).execute()
        if resp.error:
            print("[error] upsert failed:", resp.error, file=sys.stderr)
            sys.exit(1)
        total += len(chunk)
        print(f"[apply] upserted {len(chunk)} (total {total})")
        time.sleep(0.05)

    print(f"[done] applied {total} rows from {args.csv_in}")

if __name__ == "__main__":
    main()
