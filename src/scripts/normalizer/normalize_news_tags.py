#!/usr/bin/env python3
import os, math, re, time, sys, argparse, json, csv
from typing import Any, Dict, List, Optional, Tuple
from dotenv import load_dotenv, find_dotenv
from supabase import create_client, Client

# Config
TABLE = "idx_news"
PK = "id"
PAGE_SIZE = 1000
SEL_COLS = f"{PK},tags"  # minimal fetch

# Helpers
def hyphenize(s: str) -> str:
    """Normalize to kebab-like: spaces/underscores/symbols -> '-', collapse repeats."""
    s = re.sub(r"[_\s]+", "-", str(s).strip())
    s = re.sub(r"[^0-9A-Za-z-]+", "-", s)
    s = re.sub(r"-{2,}", "-", s)
    return s

# Variants we consider equivalent to "insider trading"
INSIDER_KEYS = {
    "insider-trading", "insider-tradings", "insider", "insider-trader", "insider-trade",
    "insider trading", "insider tradings", "insider trade",
}
# Remove-only tags
DROP_BUYSELL = {"buy", "sell"}

def to_list(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    return [str(raw)]

def normalize_news_tags(raw_tags: Any) -> Tuple[List[str], bool, bool, bool]:
    """
    Returns (new_tags, changed, had_insider, removed_buy_sell).
    Rules:
      - If any tag is an insider-trading variant -> ["Insider Trading"]
      - Else, drop 'buy'/'sell' (case/space/hyphen insensitive), keep others as-is
    """
    orig_list = to_list(raw_tags)
    # Fast path: empty
    if not orig_list:
        return ([], False, False, False)

    # Build comparable keys
    keys = [hyphenize(t).lower() for t in orig_list]

    had_insider = any(k in INSIDER_KEYS for k in keys)
    if had_insider:
        new_tags = ["Insider Trading"]
        changed = (orig_list != new_tags)
        return (new_tags, changed, True, False)

    # Otherwise, remove buy/sell
    removed_buy_sell = False
    new_list: List[str] = []
    for raw, key in zip(orig_list, keys):
        if key in DROP_BUYSELL:
            removed_buy_sell = True
            continue
        new_list.append(raw)

    changed = (orig_list != new_list)
    return (new_list, changed, False, removed_buy_sell)

# Main
def main():
    parser = argparse.ArgumentParser(
        description="Normalize idx_news.tags from .env credentials (UPDATE-only)."
    )
    parser.add_argument("--page-size", type=int, default=PAGE_SIZE)
    parser.add_argument("--dry-run", action="store_true", help="Scan & show counts, no writes")
    parser.add_argument("--report-csv", help="Write a CSV with id, old_tags, new_tags, changed, used_insider_rule, removed_buy_sell")
    parser.add_argument("--all-rows", action="store_true", help="Report includes unchanged rows too")
    args = parser.parse_args()

    load_dotenv(find_dotenv(usecwd=True))
    url = os.getenv("SUPABASE_URL"); key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        print("ERROR: Missing SUPABASE_URL or SUPABASE_KEY in .env", file=sys.stderr)
        sys.exit(1)

    supabase: Client = create_client(url, key)

    # total rows
    res = supabase.table(TABLE).select("*", count="exact", head=True).execute()
    total = res.count or 0
    print(f"[info] {TABLE}: total rows = {total}")

    pages = math.ceil(total / args.page_size) if total else 0
    scanned = 0
    updated = 0

    # stats
    insider_rule_cnt = 0
    removed_buy_sell_cnt = 0

    # CSV
    csv_writer = None
    csv_file = None
    if args.report_csv:
        csv_file = open(args.report_csv, "w", newline="", encoding="utf-8")
        csv_writer = csv.DictWriter(
            csv_file,
            fieldnames=[PK, "old_tags", "new_tags", "changed", "used_insider_rule", "removed_buy_sell"]
        )
        csv_writer.writeheader()

    try:
        for p in range(pages):
            start = p * args.page_size
            end = start + args.page_size - 1

            r = (
                supabase.table(TABLE)
                .select(SEL_COLS)
                .order(PK, desc=False)
                .range(start, end)
                .execute()
            )
            rows: List[Dict[str, Any]] = r.data or []
            if not rows:
                break

            page_updated = 0
            for row in rows:
                scanned += 1
                old_tags = row.get("tags")

                new_tags, changed, used_insider_rule, removed_buy_sell = normalize_news_tags(old_tags)

                if used_insider_rule:
                    insider_rule_cnt += 1
                if removed_buy_sell:
                    removed_buy_sell_cnt += 1

                if csv_writer and (args.all_rows or changed):
                    csv_writer.writerow({
                        PK: row[PK],
                        "old_tags": json.dumps(old_tags, ensure_ascii=False),
                        "new_tags": json.dumps(new_tags, ensure_ascii=False),
                        "changed": changed,
                        "used_insider_rule": used_insider_rule,
                        "removed_buy_sell": removed_buy_sell,
                    })

                if changed and not args.dry_run:
                    resp = (
                        supabase.table(TABLE)
                        .update({"tags": new_tags})
                        .eq(PK, row[PK])
                        .execute()
                    )
                    if resp.data and len(resp.data) > 0:
                        page_updated += 1
                    else:
                        print(f"[warn] update skipped: id={row[PK]} not found", file=sys.stderr)

            updated += page_updated
            print(f"[page {p+1}/{pages}] scanned={len(rows)} updated={0 if args.dry_run else page_updated}")
            time.sleep(0.05)

    finally:
        if csv_file:
            csv_file.close()

    print(f"[stats] insider_rule_applied={insider_rule_cnt}, removed_buy_sell_only={removed_buy_sell_cnt}")
    print(f"[done] scanned={scanned}, updated={0 if args.dry_run else updated}, dry_run={args.dry_run}")
    if args.report_csv:
        print(f"[report] CSV saved -> {args.report_csv}")

if __name__ == "__main__":
    main()
