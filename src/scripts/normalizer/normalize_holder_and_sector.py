#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Normalize holder_type (only 'insider' or 'institution') and kebab-case sector/sub_sector.
Update-only; supports dry-run and CSV report.

Examples:
  # Cek dulu tanpa update
  python normalize_holder_and_sectors.py --dry-run --report-csv report.csv

  # Jalankan update beneran
  python normalize_holder_and_sectors.py
"""
import os, re, sys, time, math, csv, json, argparse
from typing import Any, Dict, List, Optional, Tuple
from dotenv import load_dotenv, find_dotenv
from supabase import create_client, Client

# ----------------------
# Defaults
# ----------------------
TABLE_DEFAULT          = "idx_filings"
PK_DEFAULT             = "id"
HOLDER_COL_DEFAULT     = "holder_type"
SECTOR_COL_DEFAULT     = "sector"
SUB_SECTOR_COL_DEFAULT = "sub_sector"
PAGE_SIZE_DEFAULT      = 1000

# ----------------------
# Text helpers
# ----------------------
_WS = re.compile(r"\s+", flags=re.UNICODE)
_PUNCT = re.compile(r"[^\w\s\-&/]+", flags=re.UNICODE)

def _norm_text(x: Any) -> str:
    """Trim + collapse whitespace + strip control chars."""
    if x is None:
        return ""
    t = str(x)
    t = t.replace("\u200b", "").replace("\ufeff", "")
    t = t.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    t = _WS.sub(" ", t).strip()
    return t

def _keyize(x: Any) -> str:
    """Lowercase + strip punctuation for matching buckets."""
    t = _norm_text(x).lower()
    t = _PUNCT.sub("", t)
    t = _WS.sub(" ", t).strip()
    return t

# ----------------------
# holder_type normalization
# ----------------------
INSIDER      = "insider"
INSTITUTION  = "institution"

INSIDER_KEYS = {
    "insider", "insiders", "inside",
    "mgmt", "management", "key management", "key managerial", "kmp",
    "director", "directors", "board of directors", "bod",
    "commissioner", "commissioners", "board of commissioners", "boc",
    "officer", "officers", "key officer",
    "employee", "employees", "staff",
    "executive", "executives",
    "management and employees", "management & employees",
}

INSTITUTION_KEYS = {
    "institution", "institutional",
    "company", "corporate", "corp", "organization",
    "foundation", "yayasan",
    "bank", "fund", "mutual fund",
    "securities company", "asset manager",
    "pension fund", "insurance",
}

NULL_KEYS = {"", "-", "—", "n/a", "na", "none", "null", "unknown", "undefined"}

def normalize_holder_type(raw: Any, strict: bool=False) -> Tuple[Any, bool]:
    """Normalize holder_type into 'insider' or 'institution'."""
    original = raw
    norm = _norm_text(raw)
    k = _keyize(norm)

    if k in NULL_KEYS:
        return (None, original is not None and original != "")

    if k in INSIDER_KEYS:
        return (INSIDER, norm != INSIDER)
    if k in INSTITUTION_KEYS:
        return (INSTITUTION, norm != INSTITUTION)

    if strict:
        return (None, original is not None)
    return (original, False)

# ----------------------
# kebab-case converter for sector/sub_sector
# ----------------------
def to_kebab_case(raw: Any) -> Optional[str]:
    """
    Convert to lowercase kebab-case.
    Examples:
      "Basic Materials" -> "basic-materials"
      "Oil & Gas" -> "oil-gas"
      "Consumer Non Cyclicals" -> "consumer-non-cyclicals"
    """
    s = _norm_text(raw)
    if not s or _keyize(s) in NULL_KEYS:
        return None
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s)
    s = s.strip("-")
    return s or None

# ----------------------
# Main
# ----------------------
def main():
    ap = argparse.ArgumentParser(description="Normalize holder_type + kebab-case sector/sub_sector (update-only).")
    ap.add_argument("--table", default=TABLE_DEFAULT)
    ap.add_argument("--pk", default=PK_DEFAULT)
    ap.add_argument("--holder-col", default=HOLDER_COL_DEFAULT)
    ap.add_argument("--sector-col", default=SECTOR_COL_DEFAULT)
    ap.add_argument("--sub-sector-col", default=SUB_SECTOR_COL_DEFAULT)
    ap.add_argument("--page-size", type=int, default=PAGE_SIZE_DEFAULT)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--report-csv", help="Write CSV report with old/new values")
    ap.add_argument("--all-rows", action="store_true")
    ap.add_argument("--strict-holder", action="store_true", help="Unknown holder_type -> NULL")
    args = ap.parse_args()

    # env
    load_dotenv(find_dotenv(usecwd=True))
    url = os.getenv("SUPABASE_URL"); key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        print("ERROR: Missing SUPABASE_URL or SUPABASE_KEY in .env", file=sys.stderr)
        sys.exit(1)

    supabase: Client = create_client(url, key)

    res = supabase.table(args.table).select("*", count="exact", head=True).execute()
    total = res.count or 0
    pages = math.ceil(total / args.page_size) if total else 0
    print(f"[info] {args.table}: total={total} | pk={args.pk} | cols=[{args.holder_col}, {args.sector_col}, {args.sub_sector_col}]")

    writer = None
    csv_f = None
    if args.report_csv:
        csv_f = open(args.report_csv, "w", newline="", encoding="utf-8")
        writer = csv.DictWriter(csv_f, fieldnames=[
            args.pk, "old_holder_type", "new_holder_type",
            "old_sector", "new_sector",
            "old_sub_sector", "new_sub_sector",
            "changed", "action"
        ])
        writer.writeheader()

    scanned = 0
    updated_total = 0

    try:
        for p in range(pages):
            start = p * args.page_size
            end = start + args.page_size - 1
            cols = f"{args.pk},{args.holder_col},{args.sector_col},{args.sub_sector_col}"
            r = (
                supabase.table(args.table)
                .select(cols)
                .order(args.pk, desc=False)
                .range(start, end)
                .execute()
            )
            rows: List[Dict[str, Any]] = r.data or []
            if not rows:
                break

            page_updates = 0
            for row in rows:
                scanned += 1
                row_id = row.get(args.pk)
                old_holder = row.get(args.holder_col)
                old_sector = row.get(args.sector_col)
                old_subsec = row.get(args.sub_sector_col)

                new_holder, holder_changed = normalize_holder_type(old_holder, strict=args.strict_holder)
                new_sector = to_kebab_case(old_sector)
                new_subsec = to_kebab_case(old_subsec)

                sector_changed = new_sector != old_sector
                subsec_changed = new_subsec != old_subsec
                row_changed = holder_changed or sector_changed or subsec_changed

                if writer and (args.all_rows or row_changed):
                    writer.writerow({
                        args.pk: row_id,
                        "old_holder_type": json.dumps(old_holder, ensure_ascii=False),
                        "new_holder_type": json.dumps(new_holder, ensure_ascii=False),
                        "old_sector": json.dumps(old_sector, ensure_ascii=False),
                        "new_sector": json.dumps(new_sector, ensure_ascii=False),
                        "old_sub_sector": json.dumps(old_subsec, ensure_ascii=False),
                        "new_sub_sector": json.dumps(new_subsec, ensure_ascii=False),
                        "changed": row_changed,
                        "action": "update" if (row_changed and not args.dry_run) else ("dry-run" if row_changed else "skip")
                    })

                if not row_changed or args.dry_run:
                    continue

                payload: Dict[str, Any] = {}
                if holder_changed:
                    payload[args.holder_col] = new_holder
                if sector_changed:
                    payload[args.sector_col] = new_sector
                if subsec_changed:
                    payload[args.sub_sector_col] = new_subsec

                resp = (
                    supabase.table(args.table)
                    .update(payload)
                    .eq(args.pk, row_id)
                    .execute()
                )
                if resp.data:
                    page_updates += 1
                else:
                    print(f"[warn] update skipped id={row_id}", file=sys.stderr)

            updated_total += page_updates
            print(f"[page {p+1}/{pages}] scanned={len(rows)} updated={0 if args.dry_run else page_updates}")
            time.sleep(0.05)
    finally:
        if csv_f:
            csv_f.close()

    print("\n[done]")
    print(f"  scanned = {scanned}")
    print(f"  updated = {0 if args.dry_run else updated_total}")
    print(f"  dry_run = {args.dry_run}")
    if args.report_csv:
        print(f"[report] CSV saved → {args.report_csv}")

if __name__ == "__main__":
    main()
