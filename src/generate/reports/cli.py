from __future__ import annotations
import argparse
import asyncio
import json
import os
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from jinja2 import Template

from .utils.logger import get_logger
from .utils.datetimes import resolve_window
from .core import (
    group_report,
    load_companies_from_json,
    load_filings_from_json,
    filter_filings_by_window,
    Filing,
)
from .utils.company_map import load_company_map, annotate_holder_tickers
from .mailer import send_attachments  # SES sender

# === NEW: filters helper (watchlist-first symbols + final sweep) ===
from .utils.filters import (
    resolve_symbols_priority,
    filter_filings_by_symbols,
    filter_company_rows_by_board,
)

LOGO_PNG_PATH = "public/img/sectors-logo.png"
logger = get_logger("report.cli")

# Template helpers
def _load_template(path: str) -> str:
    p = Path(path)
    if p.exists():
        return p.read_text(encoding="utf-8")
    # fallbacks
    for fp in [
        "src/generate/reports/templates/insider_email.html",
        "generate/reports/templates/insider_email.html",
    ]:
        q = Path(fp)
        if q.exists():
            return q.read_text(encoding="utf-8")
    raise FileNotFoundError(f"Template not found: {path}")

def _render_email(html_tpl: str, ctx: Dict[str, Any]) -> str:
    tpl = Template(html_tpl)
    return tpl.render(**ctx)


# Format helpers
_ABBR_STEPS: List[Tuple[float, str]] = [(1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")]

def _abbrev_number(v: Any, digits: int = 1) -> str:
    try:
        if v is None or v == "":
            return "—"
        x = float(v); sign = "-" if x < 0 else ""; x = abs(x)
        for step, suf in _ABBR_STEPS:
            if x >= step:
                val = x / step
                s = f"{val:.{digits}f}"; s = s[:-2] if s.endswith(".0") else s
                return f"{sign}{s}{suf}"
        s = f"{x:.{digits}f}"; s = s[:-2] if s.endswith(".0") else s
        return f"{sign}{s}"
    except Exception:
        return str(v)

def fmt_abbrev_plain(v: Any) -> str: return _abbrev_number(v, digits=1)
def fmt_abbrev_idr(v: Any) -> str:
    a = _abbrev_number(v, digits=1); return "IDR " + a if a != "—" else "—"
def fmt_money(v) -> str:
    if v in (None, ""): return "-"
    try: return f"{float(v):,.0f}"
    except Exception: return str(v)
def fmt_num(v) -> str:
    if v in (None, ""): return "-"
    try: return f"{float(v):,.2f}"
    except Exception: return str(v)
def fmt_pct(v) -> str:
    if v in (None, ""): return "-"
    try: return f"{float(v):.2f}%"
    except Exception: return str(v)

# -----------------------------
# Subprocess + utils
# -----------------------------
def _run(cmd: List[str]):
    logger.info("Running: %s", " ".join(cmd))
    res = subprocess.run(cmd)
    if res.returncode != 0:
        raise SystemExit(f"Command failed ({res.returncode}): {' '.join(cmd)}")

def _ensure_dirs():
    Path("data/tmp").mkdir(parents=True, exist_ok=True)
    Path("data/report").mkdir(parents=True, exist_ok=True)

def _extract_symbols(companies_path: str, *, field: str = "symbol") -> List[str]:
    rows = load_companies_from_json(companies_path)
    seen = set(); out: List[str] = []
    for r in rows:
        s = str(r.get(field, "")).strip().upper()
        if s and s not in seen:
            seen.add(s); out.append(s)
    return out

def _format_ts_human(s: str) -> str:
    if not s: return ""
    raw = str(s).strip().replace("Z", "+00:00")
    try: return datetime.fromisoformat(raw).strftime("%d/%m/%Y %H:%M:%S")
    except Exception: return raw.replace("T", " ")

def _decorate_times(grouped: dict) -> dict:
    for comp in grouped.get("companies", []):
        for f in comp.get("filings", []):
            base = f.get("timestamp") or f.get("transaction_date") or ""
            f["display_time"] = _format_ts_human(base)
    return grouped

def _to_float_or_none(v: Any) -> Optional[float]:
    if v in (None, ""): return None
    try: return float(v)
    except Exception:
        try: return float(str(v).replace(",", ""))
        except Exception: return None

def _infer_amount_from_filing(f: Filing) -> Optional[float]:
    r = f.raw or {}
    amt = _to_float_or_none(r.get("amount_transaction"))
    if amt and amt != 0: return amt
    pt = r.get("price_transaction") or {}
    if isinstance(pt, str):
        try:
            import json as _json; pt = _json.loads(pt)
        except Exception: pt = {}
    if isinstance(pt, dict):
        arr = pt.get("amount_transacted")
        if isinstance(arr, list) and arr:
            s = sum([_to_float_or_none(x) or 0.0 for x in arr])
            if s != 0: return s
    hb = _to_float_or_none(r.get("holding_before"))
    ha = _to_float_or_none(r.get("holding_after"))
    if hb is not None and ha is not None:
        diff = abs(ha - hb)
        if diff != 0: return diff
    return None

def _build_id_to_amount(filings: List[Filing]) -> Dict[int, Optional[float]]:
    return {f.id: _infer_amount_from_filing(f) for f in filings}

def _build_id_to_source(filings: List[Filing]) -> Dict[int, Optional[str]]:
    return {f.id: (f.raw or {}).get("source") for f in filings}

def _inject_amount(grouped: dict, id_to_amount: Dict[int, Optional[float]]) -> dict:
    for comp in grouped.get("companies", []):
        for f in comp.get("filings", []):
            f["amount"] = id_to_amount.get(f["id"])
    return grouped

def _inject_source(grouped: dict, id_to_source: Dict[int, Optional[str]]) -> dict:
    for comp in grouped.get("companies", []):
        for f in comp.get("filings", []):
            f["source"] = id_to_source.get(f["id"])
    return grouped

# CLI
def build_argparser():
    p = argparse.ArgumentParser(
        description="Orchestrate: export company report → fetch filings → render & (optionally) email."
    )
    # Window & time column
    p.add_argument("--from", dest="from_time", help="Window start (ISO). If omitted, resolve_window applies 17:45 rule.")
    p.add_argument("--to", dest="to_time", help="Window end (ISO).")
    p.add_argument("--ts-col", default="timestamp", help="Time column name in filings JSON.")
    p.add_argument("--ts-kind", default="timestamp", choices=["timestamp", "timestamptz"],
                   help="Column type for window formatting (affects fetch script only).")

    # Company report export
    p.add_argument("--export-company-report", action="store_true",
                   help="Export company report JSON using company_report.py.")
    p.add_argument("--company-script", default="src/scripts/company_report.py",
                   help="Path to company_report exporter script.")
    p.add_argument("--company-select", default="symbol,company_name,listing_board,tags",
                   help="Columns to select when exporting company report.")
    p.add_argument("--company-order", default="symbol.asc", help="Order for company export.")
    p.add_argument("--companies-json-in", default=None,
                   help="If provided, skip export and use this companies JSON.")
    p.add_argument("--listing-board", dest="listing_board", default=None,
                   help="Client-side filter after export (e.g., watchlist).")

    # Symbols override
    p.add_argument("--symbols", default=None,
                   help="Comma-separated symbols to fetch filings for (overrides symbols from companies JSON).")

    # Filings fetch
    p.add_argument("--fetch-filings", action="store_true",
                   help="Fetch filings JSON using fetch_filings.py.")
    p.add_argument("--fetch-script", default="src/scripts/fetch_filings.py",
                   help="Path to fetch_filings.py.")
    p.add_argument("--fetch-out", default=None,
                   help="Output path for fetched filings JSON.")
    p.add_argument("--filings-json-in", default=None,
                   help="If provided, skip fetch and use this filings JSON.")

    # Enrichment
    p.add_argument("--company-map-json", default="data/company_map.json",
                   help="Path to company_map.json for holder_ticker enrichment.")

    # Template & outputs
    p.add_argument("--template", default="src/generate/reports/templates/insider_email.html",
                   help="HTML template path.")
    p.add_argument("--out-json", default="data/report/insider_report.json",
                   help="Where to write JSON report.")
    p.add_argument("--out-html", default="data/report/insider_email.html",
                   help="Where to write rendered HTML.")
    p.add_argument("--subject", default=None, help="Email subject override.")
    p.add_argument("--dry-run", action="store_true", help="Do not send; just write files.")

    # Sending
    p.add_argument("--send", action="store_true", help="Send rendered HTML via SES.")
    p.add_argument("--mail-to", dest="to_emails", help="Recipient email(s), comma-separated (required if --send).")
    p.add_argument("--from-email", dest="from_email", default=None, help="Override From header.")
    p.add_argument("--region", dest="aws_region", default=None, help="AWS region override.")
    p.add_argument("--cc", default=None); p.add_argument("--bcc", default=None)
    p.add_argument("--reply-to", dest="reply_to", default=None)
    p.add_argument("--attach-json", action="store_true"); p.add_argument("--attach-html", action="store_true")
    return p

# -----------------------------
# Main flow
# -----------------------------
async def run(args):
    _ensure_dirs()

    # 1) Resolve window
    win = resolve_window(args.from_time, args.to_time)
    logger.info("Window JKT: %s -> %s", win.start.isoformat(), win.end.isoformat())

    # 2) Company report (optional)
    companies_json_path = args.companies_json_in
    if args.export_company_report and not companies_json_path:
        companies_json_path = f"data/tmp/company_report_{win.end.strftime('%Y%m%d')}.json"
        _run([
            sys.executable, args.company_script,
            "--select", args.company_select,
            "--order", args.company_order,
            "--out-json", companies_json_path,
        ])
        logger.info("Company report exported: %s", companies_json_path)

        # client-side filter by listing_board (e.g. watchlist)
        if args.listing_board and os.path.exists(companies_json_path):
            with open(companies_json_path, "r", encoding="utf-8") as f:
                rows = json.load(f) or []
            want = (args.listing_board or "").strip().lower()
            rows = [r for r in rows if str(r.get("listing_board", "")).strip().lower() == want]
            with open(companies_json_path, "w", encoding="utf-8") as f:
                json.dump(rows, f, ensure_ascii=False, indent=2)
            logger.info("Company report filtered by listing_board=%s: %d rows", want, len(rows))

    # 3) Resolve symbols ( --symbols > watchlist > insider )
    symbols: List[str] = await resolve_symbols_priority(
        symbols_arg=(args.symbols or None),
        use_company_report_watchlist=True,
        companies_json_in=(companies_json_path or args.companies_json_in),
        symbol_col="symbol",
    )
    logger.info("Symbols resolved: %d -> sample=%s", len(symbols), symbols[:10] if symbols else [])

    # 4) Filings (fetch or offline)
    filings_json_path = args.filings_json_in
    if args.fetch_filings and not filings_json_path:
        filings_json_path = args.fetch_out or f"data/tmp/filings_{win.start.strftime('%Y-%m-%d_%H%M')}__{win.end.strftime('%Y-%m-%d_%H%M')}.json"
        cmd = [
            sys.executable, args.fetch_script,
            "--from", win.start.strftime("%Y-%m-%d %H:%M"),
            "--to",   win.end.strftime("%Y-%m-%d %H:%M"),
            "--ts-col", args.ts_col, "--ts-kind", args.ts_kind,
            "--out", filings_json_path,
        ]
        # PASS symbols to fetcher → server-side filter (symbol=in.(...))
        if symbols:
            cmd += ["--symbols", ",".join(symbols)]
        logger.info("Running fetch: %s", " ".join(shlex.quote(c) for c in cmd))
        _run(cmd)
        logger.info("Filings fetched: %s", filings_json_path)

    if not filings_json_path or not os.path.exists(filings_json_path):
        raise SystemExit("No filings JSON provided. Use --fetch-filings or --filings-json-in.")

    # 4b) Apply window & final sweep (client-side safety)
    all_filings = load_filings_from_json(filings_json_path)
    filings: List[Filing] = filter_filings_by_window(all_filings, win, ts_col=args.ts_col, ts_kind=args.ts_kind)
    logger.info("Filings (offline): %d in window", len(filings))
    # keep only watchlist symbols (if any)
    filings = filter_filings_by_symbols(filings, symbols)

    # 5) Enrichment
    try:
        _, by_name = load_company_map(args.company_map_json)
        n_filled = annotate_holder_tickers(filings, by_name)
        logger.info("Holder ticker enriched from company_map: %d rows updated", n_filled)
    except FileNotFoundError:
        logger.warning("company_map.json not found at %s — skipping enrichment", args.company_map_json)
    except Exception as e:
        logger.error("Failed enriching holder_ticker from company_map: %s", e, exc_info=True)

    id_to_amount = _build_id_to_amount(filings)
    id_to_source = _build_id_to_source(filings)

    # 6) Group & decorate
    grouped = group_report(filings)
    grouped = _decorate_times(grouped)
    grouped = _inject_amount(grouped, id_to_amount)
    grouped = _inject_source(grouped, id_to_source)

    # (opsional) filter company_rows untuk tampilan kalau kamu ikut load sendiri
    # NOTE: group_report() kamu tidak butuh company_rows eksplisit. Jika kamu ingin
    # menampilkan tabel perusahaan terfilter board, tambah logic load+filter di sini:
    # company_rows = load_companies_from_json(companies_json_path) if companies_json_path else []
    # if company_rows and args.listing_board:
    #     company_rows = filter_company_rows_by_board(company_rows, args.listing_board)

    # 7) Write JSON
    # Write resolve window start and end to json 
    grouped['window_start'] = win.start.strftime("%Y-%m-%d %H:%M:%S")
    grouped['window_end'] = win.end.strftime("%Y-%m-%d %H:%M:%S")

    Path(args.out_json).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_json).write_text(json.dumps(grouped, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Wrote JSON report: %s", args.out_json)

    # 8) Render HTML
    subject = args.subject or f"[IDX] Insider Filings — {win.start.strftime('%Y-%m-%d %H:%M')} → {win.end.strftime('%Y-%m-%d %H:%M')} WIB"
    logo_cid = None; inline_imgs: List[Tuple[str, str]] = []
    if os.path.exists(LOGO_PNG_PATH):
        logo_cid = "sectors_logo"; inline_imgs.append((logo_cid, LOGO_PNG_PATH))
    ctx = {
        "subject": subject,
        "window_start": win.start.strftime("%Y-%m-%d %H:%M:%S"),
        "window_end": win.end.strftime("%Y-%m-%d %H:%M:%S"),
        "totals": {"total_companies": grouped["total_companies"], "total_filings": grouped["total_filings"]},
        "companies": grouped["companies"],
        "fmt_money": fmt_money, "fmt_num": fmt_num, "fmt_pct": fmt_pct,
        "fmt_abbrev_plain": fmt_abbrev_plain, "fmt_abbrev_idr": fmt_abbrev_idr,
        "logo_cid": logo_cid,
    }
    html = _render_email(_load_template(args.template), ctx)
    Path(args.out_html).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_html).write_text(html, encoding="utf-8")
    logger.info("Wrote HTML email: %s", args.out_html)

    # 9) Optional send
    if args.send and not args.dry_run:
        if not args.to_emails:
            raise SystemExit("--send requires --mail-to <recipient(s)>")
        attachments: List[str] = []
        if args.attach_json: attachments.append(args.out_json)
        if args.attach_html: attachments.append(args.out_html)
        res = send_attachments(
            to=args.to_emails, subject=subject,
            body_text="Plain-text fallback. Please view the HTML version.",
            body_html=html, files=attachments,
            from_email=args.from_email, cc=args.cc, bcc=args.bcc,
            reply_to=args.reply_to, aws_region=args.aws_region,
            inline_images=inline_imgs,
        )
        status = "OK" if res.get("ok") else "FAIL"
        logger.info("Send status=%s, message_id=%s, error=%s", status, res.get("message_id"), res.get("error"))
        print(json.dumps(res, indent=2))

def main():
    args = build_argparser().parse_args()
    asyncio.run(run(args))

if __name__ == "__main__":
    main()
