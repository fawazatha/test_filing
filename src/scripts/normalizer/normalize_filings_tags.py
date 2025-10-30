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
REASON_TAGS = {
    "divestment", "investment", "free-float-requirement",
    "MESOP", "inheritance", "share-transfer"
}
CONTROL_TAGS = {"takeover"}
WHITELIST = SENTIMENTS | REASON_TAGS | CONTROL_TAGS

# ----------------------
# Helpers
# ----------------------
def hyphenize(s: str) -> str:
    """Normalize to kebab-like: spaces/underscores/symbols -> '-', collapse repeats."""
    s = re.sub(r"[_\s]+", "-", s.strip())
    s = re.sub(r"[^0-9A-Za-z-]+", "-", s)
    s = re.sub(r"-{2,}", "-", s)
    return s

VARIANTS = {
    # reason tags
    "mesop": "MESOP",
    "mesop-program": "MESOP",
    "free-float-requirement": "free-float-requirement",
    "freefloatrequirement": "free-float-requirement",
    "free-float": "free-float-requirement",
    "free-float-req": "free-float-requirement",
    "freefloat": "free-float-requirement",
    "freefloat-req": "free-float-requirement",
    "free float requirement": "free-float-requirement",
    "share-transfer": "share-transfer",
    "sharetransfer": "share-transfer",
    "share transfer": "share-transfer",
    "investment": "investment",
    "invest": "investment",
    "divestment": "divestment",
    "divest": "divestment",
    "inheritance": "inheritance",
    "inheritence": "inheritance",
    # sentiments
    "bullish": "bullish",
    "bull": "bullish",
    "bearish": "bearish",
    "bear": "bearish",
    # control
    "takeover": "takeover",
    "take-over": "takeover",
}

_DROP_SET = {
    # explicit drops / noise (akan dibuang di canonicalize_one)
    "insider-trading", "insider", "insider-tradings", "insider trading",
    "ownership-change", "ownership-changes", "ownership change", "ownership changes",
    "executive-shareholding-changes", "executive shareholding changes",
    "idx"
}

def canonicalize_one(tag: str) -> Optional[str]:
    """Map raw tag ke kanonikal; return None jika harus dibuang."""
    k = hyphenize(str(tag)).lower()
    if k in _DROP_SET:
        return None
    if k == "mesop":
        return "MESOP"
    mapped = VARIANTS.get(k)
    if mapped is None:
        return None
    return "MESOP" if mapped.lower() == "mesop" else mapped

def keep_only_reason_tags(raw_tags: Any) -> List[str]:
    """Ambil hanya reason tags dari raw_tags (setelah canonicalization)."""
    if raw_tags is None:
        arr: List[str] = []
    elif isinstance(raw_tags, list):
        arr = [str(x) for x in raw_tags]
    else:
        arr = [str(raw_tags)]
    mapped = [canonicalize_one(t) for t in arr]
    return [t for t in mapped if t in REASON_TAGS]

def parse_pct(x: Any) -> Optional[float]:
    """Parse '12.5', '12.5%', '1,234.56' -> float; None jika gagal."""
    if x is None:
        return None
    try:
        s = str(x).strip()
        s = s.replace('%', '').replace(',', '')
        if s == "":
            return None
        return float(s)
    except Exception:
        return None

def derive_sentiment(before: Any, after: Any) -> Optional[str]:
    b = parse_pct(before)
    a = parse_pct(after)
    if b is None or a is None:
        return None
    if a > b:
        return "bullish"
    if a < b:
        return "bearish"
    return None

def takeover(before: Any, after: Any) -> bool:
    b = parse_pct(before)
    a = parse_pct(after)
    if b is None or a is None:
        return False
    return (b < 50.0 and a > 50.0) or (b > 50.0 and a < 50.0)  

def assemble_new_tags(existing: Any, before: Any, after: Any) -> List[str]:
    """Kembalikan daftar tag akhir (whitelist-only), unik, disortir (MESOP dulu)."""
    out = keep_only_reason_tags(existing)
    s = derive_sentiment(before, after)
    if s:
        out.append(s)
    if takeover(before, after):
        out.append("takeover")
    seen = set(); uniq = []
    for t in out:
        if t in WHITELIST and t not in seen:
            uniq.append(t); seen.add(t)
    uniq.sort(key=lambda x: (x != "MESOP", x.lower()))
    return uniq

def _sorted_canonical(arr: List[str]) -> List[str]:
    """Sort helper untuk perbandingan deterministik (MESOP dulu)."""
    return sorted([str(x) for x in arr], key=lambda x: (x != "MESOP", x.lower()))

# ----------------------
# Main
# ----------------------
def main():
    parser = argparse.ArgumentParser(description="Normalize idx_filings.tags from .env credentials (UPDATE-only).")
    parser.add_argument("--page-size", type=int, default=PAGE_SIZE)
    parser.add_argument("--dry-run", action="store_true", help="Scan & show counts, no writes")
    parser.add_argument("--report-csv", help="Write a CSV with id, old_tags, new_tags, changed, legacy_null")
    parser.add_argument("--all-rows", action="store_true", help="Report includes unchanged rows too")
    args = parser.parse_args()

    load_dotenv(find_dotenv(usecwd=True))
    url = os.getenv("SUPABASE_URL"); key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        print("ERROR: Missing SUPABASE_URL or SUPABASE_KEY in .env", file=sys.stderr)
        sys.exit(1)

    supabase: Client = create_client(url, key)

    # hitung total rows
    res = supabase.table(TABLE).select("*", count="exact", head=True).execute()
    total = res.count or 0
    print(f"[info] {TABLE}: total rows = {total}")

    pages = math.ceil(total / args.page_size) if total else 0
    scanned = 0
    updated = 0

    # statistik tambahan
    legacy_null_cnt = 0
    sentiment_cnt = 0
    takeover_cnt = 0

    # CSV writer (opsional)
    csv_writer = None
    csv_file = None
    if args.report_csv:
        csv_file = open(args.report_csv, "w", newline="", encoding="utf-8")
        csv_writer = csv.DictWriter(
            csv_file,
            fieldnames=[PK, "old_tags", "new_tags", "changed", "legacy_null"]
        )
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
            if not rows:
                break

            # kumpulkan perubahan dalam satu halaman
            batch_updates: List[Dict[str, Any]] = []
            page_updated = 0

            for row in rows:
                scanned += 1
                old_tags = row.get("tags")
                before = row.get(COL_BEFORE)
                after  = row.get(COL_AFTER)

                # stats
                is_legacy = (parse_pct(before) is None and parse_pct(after) is None)
                if is_legacy:
                    legacy_null_cnt += 1
                s = derive_sentiment(before, after)
                if s:
                    sentiment_cnt += 1
                if takeover(before, after):
                    takeover_cnt += 1

                # compute tags baru
                new_tags = assemble_new_tags(old_tags, before, after)

                # === PENTING: bandingkan RAW vs NEW (bukan old_norm) ===
                if old_tags is None:
                    old_raw = []
                elif isinstance(old_tags, list):
                    old_raw = [str(x) for x in old_tags]
                else:
                    old_raw = [str(old_tags)]

                old_raw_sorted = _sorted_canonical(old_raw)
                new_sorted = _sorted_canonical(new_tags)
                changed = (old_raw_sorted != new_sorted)

                # tulis CSV (opsional)
                if csv_writer and (args.all_rows or changed):
                    csv_writer.writerow({
                        PK: row[PK],
                        "old_tags": json.dumps(old_tags, ensure_ascii=False),
                        "new_tags": json.dumps(new_tags, ensure_ascii=False),
                        "changed": changed,
                        "legacy_null": is_legacy,
                    })

                # queue update kalau berubah
                if changed:
                    batch_updates.append({PK: row[PK], "tags": new_tags})

            # === UPDATE-ONLY (no insert) ===
            if batch_updates and not args.dry_run:
                for item in batch_updates:
                    row_id = item[PK]
                    new_tags = item["tags"]
                    resp = (
                        supabase.table(TABLE)
                        .update({"tags": new_tags})
                        .eq(PK, row_id)
                        .execute()
                    )
                    if resp.data and len(resp.data) > 0:
                        page_updated += 1
                    else:
                        print(f"[warn] update skipped: id={row_id} not found", file=sys.stderr)
                updated += page_updated

            print(f"[page {p+1}/{pages}] scanned={len(rows)} updated={0 if args.dry_run else page_updated}")
            time.sleep(0.05)

    finally:
        if csv_file:
            csv_file.close()

    # ringkasan
    print(f"[stats] legacy_null={legacy_null_cnt}, sentiment_added={sentiment_cnt}, added={takeover_cnt}")
    print(f"\n[done] scanned={scanned}, updated={0 if args.dry_run else updated}, dry_run={args.dry_run}")
    if args.report_csv:
        print(f"[report] CSV saved → {args.report_csv}")

if __name__ == "__main__":
    main()
