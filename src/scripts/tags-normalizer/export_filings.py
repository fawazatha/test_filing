#!/usr/bin/env python3
import os, math, csv, sys, argparse, json
from typing import Any, Dict, List
from dotenv import load_dotenv, find_dotenv
from supabase import create_client, Client

def main():
    ap = argparse.ArgumentParser(description="Export idx_filings {id,tags[,tags_backup]} to CSV")
    ap.add_argument("--table", default="idx_filings")
    ap.add_argument("--pk", default="id")
    ap.add_argument("--page-size", type=int, default=1000)
    ap.add_argument("--include-backup", action="store_true", help="Also export tags_backup if exists")
    ap.add_argument("--out", default="idx_filings_tags_export.csv")
    args = ap.parse_args()

    load_dotenv(find_dotenv(usecwd=True))
    url = os.getenv("SUPABASE_URL"); key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        print("ERROR: Missing SUPABASE_URL or SUPABASE_KEY in .env", file=sys.stderr); sys.exit(1)

    supabase: Client = create_client(url, key)

    # Count
    r = supabase.table(args.table).select("*", count="exact", head=True).execute()
    total = r.count or 0
    pages = math.ceil(total / args.page_size) if total else 0
    cols = [args.pk, "tags"] 

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        written = 0

        for p in range(pages):
            start, end = p * args.page_size, p * args.page_size + args.page_size - 1
            sel = ",".join(cols)
            resp = supabase.table(args.table).select(sel).range(start, end).execute()
            rows: List[Dict[str, Any]] = resp.data or []
            if not rows: break

            for row in rows:
                rec = {args.pk: row[args.pk]}
                # Keep tags as compact JSON so it round-trips safely (works for text[] or jsonb)
                rec["tags"] = json.dumps(row.get("tags", []), ensure_ascii=False)
                if args.include_backup:
                    rec["tags_backup"] = json.dumps(row.get("tags_backup"), ensure_ascii=False)
                w.writerow(rec)
                written += 1

            print(f"[export] page {p+1}/{pages} wrote {len(rows)} rows")

    print(f"[done] CSV saved → {args.out} (rows: {written})")

if __name__ == "__main__":
    main()
