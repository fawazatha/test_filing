#!/usr/bin/env python3
import os, math, re, time, sys, argparse, json, csv
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv, find_dotenv
from supabase import create_client, Client

# ----------------------
# Config
# ----------------------
TABLE = "idx_filings"
PK = "id"
PAGE_SIZE = 1000
COL_BEFORE = "share_percentage_before"
COL_AFTER  = "share_percentage_after"

# ----------------------
# Canonical tag policy
# ----------------------
SENTIMENTS = {"bullish", "bearish"}
REASON_TAGS = {"divestment","investment","free-float-requirement","MESOP","inheritance","share-transfer"}
CONTROL_TAGS = {"takeover"}
DROP_EXPLICIT = {"insider-trading", "ownership-change"}
WHITELIST = SENTIMENTS | REASON_TAGS | CONTROL_TAGS

def hyphenize(s: str) -> str:
    s = re.sub(r"[_\s]+", "-", s.strip())
    s = re.sub(r"-{2,}", "-", s)
    return s

VARIANTS = {
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
    "bullish": "bullish",
    "bull": "bullish",
    "bearish": "bearish",
    "bear": "bearish",
    "takeover": "takeover",
    "take-over": "takeover",
}

def canonicalize_one(tag: str) -> Optional[str]:
    k = hyphenize(str(tag)).lower()
    if k in {"insider-trading", "insider", "insider trading", "ownership-change", "ownership change"}:
        return None
    if k == "mesop":
        return "MESOP"
    mapped = VARIANTS.get(k)
    if mapped is None:
        return None
    return "MESOP" if mapped.lower() == "mesop" else mapped

def keep_only_reason_tags(raw_tags: Any) -> List[str]:
    if raw_tags is None:
        arr: List[str] = []
    elif isinstance(raw_tags, list):
        arr = [str(x) for x in raw_tags]
    else:
        arr = [str(raw_tags)]
    mapped = [canonicalize_one(t) for t in arr]
    return [t for t in mapped if t in REASON_TAGS]

def derive_sentiment(before: Any, after: Any) -> Optional[str]:
    try:
        b = float(before) if before is not None else None
        a = float(after)  if after  is not None else None
    except Exception:
        return None
    if b is None or a is None:
        return None
    if a > b: return "bullish"
    if a < b: return "bearish"
    return None

def takeover_upward_only(before: Any, after: Any) -> bool:
    try:
        b = float(before) if before is not None else None
        a = float(after)  if after  is not None else None
    except Exception:
        return False
    if b is None or a is None:
        return False
    return (b < 50.0) and (a > 50.0)  # upward crossing only

def assemble_new_tags(existing: Any, before: Any, after: Any) -> List[str]:
    out = keep_only_reason_tags(existing)
    s = derive_sentiment(before, after)
    if s: out.append(s)
    if takeover_upward_only(before, after): out.append("takeover")
    seen = set(); uniq = []
    for t in out:
        if t in WHITELIST and t not in seen:
            uniq.append(t); seen.add(t)
    uniq.sort(key=lambda x: (x != "MESOP", x.lower()))
    return uniq

def normalize_old_for_compare(old_tags: Any) -> List[str]:
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

def main():
    parser = argparse.ArgumentParser(description="Normalize idx_filings.tags from .env credentials")
    parser.add_argument("--page-size", type=int, default=PAGE_SIZE)
    parser.add_argument("--dry-run", action="store_true", help="Scan & show counts, no writes")
    parser.add_argument("--report-csv", help="Write a CSV with id, old_tags, new_tags, changed")
    parser.add_argument("--all-rows", action="store_true", help="Report includes unchanged rows too")
    args = parser.parse_args()

    load_dotenv(find_dotenv(usecwd=True))
    url = os.getenv("SUPABASE_URL"); key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        print("ERROR: Missing SUPABASE_URL or SUPABASE_KEY in .env", file=sys.stderr)
        sys.exit(1)

    supabase: Client = create_client(url, key)

    res = supabase.table(TABLE).select("*", count="exact", head=True).execute()
    total = res.count or 0
    print(f"[info] {TABLE}: total rows = {total}")

    pages = math.ceil(total / args.page_size) if total else 0
    scanned = 0
    updated = 0

    # prepare CSV writer if requested
    csv_writer = None
    csv_file = None
    if args.report_csv:
        csv_file = open(args.report_csv, "w", newline="", encoding="utf-8")
        csv_writer = csv.DictWriter(csv_file, fieldnames=[PK, "old_tags", "new_tags", "changed"])
        csv_writer.writeheader()

    try:
        for p in range(pages):
            start = p * args.page_size
            end = start + args.page_size - 1
            sel_cols = f"{PK},tags,{COL_BEFORE},{COL_AFTER}"
            r = (
                supabase.table(TABLE)
                .select(sel_cols)
                .order(PK, desc=False)
                .range(start, end)
                .execute()
            )
            rows: List[Dict[str, Any]] = r.data or []
            if not rows: break

            batch_updates: List[Dict[str, Any]] = []
            for row in rows:
                scanned += 1
                old_tags = row.get("tags")
                before = row.get(COL_BEFORE)
                after  = row.get(COL_AFTER)

                new_tags = assemble_new_tags(old_tags, before, after)
                old_norm = normalize_old_for_compare(old_tags)
                changed = (old_norm != new_tags)

                # write CSV report row
                if csv_writer and (args.all_rows or changed):
                    csv_writer.writerow({
                        PK: row[PK],
                        "old_tags": json.dumps(old_tags, ensure_ascii=False),
                        "new_tags": json.dumps(new_tags, ensure_ascii=False),
                        "changed": changed,
                    })

                if changed:
                    batch_updates.append({PK: row[PK], "tags": new_tags})

            if batch_updates and not args.dry_run:
                for i in range(0, len(batch_updates), 500):
                    chunk = batch_updates[i:i+500]
                    resp = supabase.table(TABLE).upsert(chunk, on_conflict=PK).execute()
                    if resp.error:
                        print("[error] upsert failed:", resp.error, file=sys.stderr)
                        sys.exit(1)
                updated += len(batch_updates)

            print(f"[page {p+1}/{pages}] scanned={len(rows)} updated={0 if args.dry_run else len(batch_updates)}")
            time.sleep(0.05)
    finally:
        if csv_file:
            csv_file.close()

    print(f"\n[done] scanned={scanned}, updated={0 if args.dry_run else updated}, dry_run={args.dry_run}")
    if args.report_csv:
        print(f"[report] CSV saved → {args.report_csv}")

if __name__ == "__main__":
    main()
