# src/generate/reports/cli.py
from __future__ import annotations
import argparse
import asyncio
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from jinja2 import Template

from .utils.logger import get_logger
from .utils.datetimes import resolve_window
from .core import (
    fetch_company_report_symbols,
    fetch_filings_for_symbols_between,
    group_report,
    load_companies_from_json,
    load_filings_from_json,
    filter_filings_by_window,
    Filing,
)
from .utils.company_map import load_company_map, annotate_holder_tickers
from .mailer import send_attachments  # SES sender

# ---- fixed path to inline PNG logo (CID) ----
LOGO_PNG_PATH = "public/img/sectors-logo.png"

logger = get_logger("report.cli")

# -----------------------------
# Template helpers
# -----------------------------
def _load_template(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def _render_email(html_tpl: str, ctx: Dict[str, Any]) -> str:
    tpl = Template(html_tpl)
    return tpl.render(**ctx)

# -----------------------------
# Abbreviation helpers (K/M/B/T)
# -----------------------------
_ABBR_STEPS: List[Tuple[float, str]] = [(1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")]

def _abbrev_number(v: Any, digits: int = 1) -> str:
    try:
        if v is None or v == "":
            return "—"
        x = float(v)
        sign = "-" if x < 0 else ""
        x = abs(x)
        for step, suf in _ABBR_STEPS:
            if x >= step:
                val = x / step
                s = f"{val:.{digits}f}"
                if s.endswith(".0"):
                    s = s[:-2]
                return f"{sign}{s}{suf}"
        s = f"{x:.{digits}f}"
        if s.endswith(".0"):
            s = s[:-2]
        return f"{sign}{s}"
    except Exception:
        return str(v)

def fmt_abbrev_plain(v: Any) -> str:
    return _abbrev_number(v, digits=1)

def fmt_abbrev_idr(v: Any) -> str:
    a = _abbrev_number(v, digits=1)
    return "IDR " + a if a != "—" else "—"

# -----------------------------
# Other numeric formatters (kept for completeness)
# -----------------------------
def fmt_money(v) -> str:
    if v is None or v == "":
        return "-"
    try:
        return f"{float(v):,.0f}"
    except Exception:
        return str(v)

def fmt_num(v) -> str:
    if v is None or v == "":
        return "-"
    try:
        return f"{float(v):,.2f}"
    except Exception:
        return str(v)

def fmt_pct(v) -> str:
    if v is None or v == "":
        return "-"
    try:
        return f"{float(v):.2f}%"
    except Exception:
        return str(v)

# -----------------------------
# Time decoration (dd/mm/YYYY HH:MM:SS)
# -----------------------------
def _format_ts_human(s: str) -> str:
    if not s:
        return ""
    raw = str(s).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return raw.replace("T", " ")

def _decorate_times(grouped: dict) -> dict:
    for comp in grouped.get("companies", []):
        for f in comp.get("filings", []):
            base = f.get("timestamp") or f.get("transaction_date") or ""
            f["display_time"] = _format_ts_human(base)
    return grouped

# -----------------------------
# Amount + source inference helpers
# -----------------------------
def _to_float_or_none(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        try:
            return float(str(v).replace(",", ""))
        except Exception:
            return None

def _infer_amount_from_filing(f: Filing) -> Optional[float]:
    """
    Priority:
      1) amount_transaction
      2) sum(price_transaction.amount_transacted[])
      3) |holding_after - holding_before|
    """
    r = f.raw or {}

    amt = r.get("amount_transaction")
    amt_f = _to_float_or_none(amt)
    if amt_f and amt_f != 0:
        return amt_f

    pt = r.get("price_transaction") or {}
    if isinstance(pt, str):
        try:
            import json as _json
            pt = _json.loads(pt)
        except Exception:
            pt = {}
    if isinstance(pt, dict):
        arr = pt.get("amount_transacted")
        if isinstance(arr, list) and len(arr) > 0:
            s = sum([_to_float_or_none(x) or 0.0 for x in arr])
            if s != 0:
                return s

    hb = _to_float_or_none(r.get("holding_before"))
    ha = _to_float_or_none(r.get("holding_after"))
    if hb is not None and ha is not None:
        diff = abs(ha - hb)
        if diff != 0:
            return diff

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

# -----------------------------
# Fetch-first helpers
# -----------------------------
def _safe_tag_from_dtstr(s: str) -> str:
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.strftime("%Y%m%d_%H%M%S")
    except Exception:
        return s.replace(":", "").replace("-", "").replace(" ", "_").replace("/", "")

def _guess_fetch_out(args) -> str:
    Path("data/tmp").mkdir(parents=True, exist_ok=True)
    start_s = args.from_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    end_s   = args.to_time   or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"data/tmp/filings_{_safe_tag_from_dtstr(start_s)}__{_safe_tag_from_dtstr(end_s)}.json"

def _run_fetch_script(args, fetch_out_path: str):
    cmd = [
        "python",
        args.fetch_script,
        "--from", args.from_time,
        "--to", args.to_time,
        "--ts-col", args.ts_col,
        "--ts-kind", args.ts_kind,
        "--out", fetch_out_path,
    ]
    logger.info("Running fetch script: %s", " ".join(cmd))
    env = os.environ.copy()
    res = subprocess.run(cmd, env=env)
    if res.returncode != 0:
        raise RuntimeError(f"fetch script failed with exit code {res.returncode}")

# -----------------------------
# CLI
# -----------------------------
def build_argparser():
    p = argparse.ArgumentParser(
        description="Fetch (optional), generate insider filings report, render HTML, and optionally email via SES."
    )
    # window & time column
    p.add_argument("--from", dest="from_time", help="Start (ISO). If omitted, defaults to now-24h.")
    p.add_argument("--to", dest="to_time", help="End (ISO). If omitted, defaults to now.")
    p.add_argument("--ts-col", default="timestamp", help="Time column in idx_filings (default: timestamp).")
    p.add_argument("--ts-kind", default="timestamp", choices=["timestamp", "timestamptz"],
                   help="Column type (default: timestamp).")
    # offline inputs
    p.add_argument("--filings-json-in", help="Read filings JSON (output fetch_filings.py) instead of Supabase.")
    p.add_argument("--companies-json-in", help="Read company report JSON instead of Supabase.")
    # fetch-first mode
    p.add_argument("--fetch-first", action="store_true",
                   help="Run fetch_filings.py first to produce a JSON, then generate report from it.")
    p.add_argument("--fetch-script", default="src/scripts/fetch_filings.py",
                   help="Path to fetch_filings.py (default).")
    p.add_argument("--fetch-out", default=None,
                   help="Output path for fetched filings JSON (default: auto under data/tmp/).")
    # enrichment
    p.add_argument("--company-map-json", default="data/company_map.json",
                   help="Path to company_map.json for holder_ticker enrichment.")
    # template & outputs
    p.add_argument("--template", default="src/generate/reports/templates/insider_email.html",
                   help="HTML template path.")
    p.add_argument("--out-json", default="data/report/insider_report.json",
                   help="Where to write JSON report.")
    p.add_argument("--out-html", default="data/report/insider_email.html",
                   help="Where to write rendered HTML.")
    p.add_argument("--subject", default=None, help="Email subject override.")
    p.add_argument("--dry-run", action="store_true", help="Do not send; just write files.")
    # sending options
    p.add_argument("--send", action="store_true", help="If set, send the rendered HTML via SES.")
    p.add_argument("--mail-to", dest="to_emails", help="Recipient email(s), comma-separated (required if --send).")
    p.add_argument("--from-email", dest="from_email", default=None,
                   help="Override From header (defaults to SES_FROM_EMAIL env).")
    p.add_argument("--region", dest="aws_region", default=None,
                   help="AWS region override (defaults to AWS_REGION/SES_REGION env).")
    p.add_argument("--cc", default=None, help="CC recipient(s), comma-separated.")
    p.add_argument("--bcc", default=None, help="BCC recipient(s), comma-separated.")
    p.add_argument("--reply-to", dest="reply_to", default=None, help="Reply-To header, comma-separated.")
    p.add_argument("--attach-json", action="store_true", help="Attach the generated JSON report.")
    p.add_argument("--attach-html", action="store_true", help="Attach the rendered HTML.")
    return p

# -----------------------------
# Main flow
# -----------------------------
async def run(args):
    # 1) Window
    win = resolve_window(args.from_time, args.to_time)
    logger.info("Window JKT: %s -> %s", win.start.isoformat(), win.end.isoformat())

    # 2) Companies
    if args.companies_json_in:
        companies = load_companies_from_json(args.companies_json_in)
        logger.info("Company report (offline): %d insider-tagged companies", len(companies))
    else:
        companies = await fetch_company_report_symbols()

    # 3a) Optional fetch-first
    if args.fetch_first and not args.filings_json_in:
        fetch_out = args.fetch_out or _guess_fetch_out(args)
        _run_fetch_script(args, fetch_out)
        args.filings_json_in = fetch_out
        logger.info("Fetch complete. Using filings JSON: %s", args.filings_json_in)

    # 3b) Filings
    if args.filings_json_in:
        all_filings = load_filings_from_json(args.filings_json_in)
        filings: List[Filing] = filter_filings_by_window(all_filings, win, ts_col=args.ts_col, ts_kind=args.ts_kind)
        logger.info("Filings (offline): %d in window", len(filings))
    else:
        symbols = [c.symbol for c in companies]
        filings = await fetch_filings_for_symbols_between(symbols, win, ts_col=args.ts_col, ts_kind=args.ts_kind)

    # 3c) Enrich holder_ticker from company_map.json
    try:
        _, by_name = load_company_map(args.company_map_json)
        n_filled = annotate_holder_tickers(filings, by_name)
        logger.info("Holder ticker enriched from company_map: %d rows updated", n_filled)
    except FileNotFoundError:
        logger.warning("company_map.json not found at %s — skipping enrichment", args.company_map_json)
    except Exception as e:
        logger.error("Failed enriching holder_ticker from company_map: %s", e, exc_info=True)

    # 3d) Derived maps
    id_to_amount = _build_id_to_amount(filings)
    id_to_source = _build_id_to_source(filings)

    # 4) Group + decorate (display_time + amount + source)
    grouped = group_report(companies, filings)
    grouped = _decorate_times(grouped)
    grouped = _inject_amount(grouped, id_to_amount)
    grouped = _inject_source(grouped, id_to_source)

    # 5) Write JSON
    Path(args.out_json).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_json).write_text(json.dumps(grouped, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Wrote JSON report: %s", args.out_json)

    # 6) Render HTML
    subject = args.subject or f"[IDX] Insider Filings — {win.start.strftime('%Y-%m-%d %H:%M')} → {win.end.strftime('%Y-%m-%d %H:%M')} WIB"

    # Inline logo via CID if file exists
    logo_cid = None
    inline_imgs: List[Tuple[str, str]] = []
    if os.path.exists(LOGO_PNG_PATH):
        logo_cid = "sectors_logo"
        inline_imgs.append((logo_cid, LOGO_PNG_PATH))

    ctx = {
        "subject": subject,
        "window_start": win.start.strftime("%Y-%m-%d %H:%M:%S"),
        "window_end": win.end.strftime("%Y-%m-%d %H:%M:%S"),
        "totals": {
            "total_companies": grouped["total_companies"],
            "total_filings": grouped["total_filings"],
        },
        "companies": grouped["companies"],
        # helpers for template
        "fmt_money": fmt_money,
        "fmt_num": fmt_num,
        "fmt_pct": fmt_pct,
        "fmt_abbrev_plain": fmt_abbrev_plain,
        "fmt_abbrev_idr": fmt_abbrev_idr,
        # CID for logo
        "logo_cid": logo_cid,
    }
    tpl = _load_template(args.template)
    html = _render_email(tpl, ctx)
    Path(args.out_html).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_html).write_text(html, encoding="utf-8")
    logger.info("Wrote HTML email: %s", args.out_html)

    # 7) Optional send
    if args.send and not args.dry_run:
        if not args.to_emails:
            raise SystemExit("--send requires --mail-to <recipient(s)>")
        attachments: List[str] = []
        if args.attach_json:
            attachments.append(args.out_json)
        if args.attach_html:
            attachments.append(args.out_html)

        res = send_attachments(
            to=args.to_emails,
            subject=subject,
            body_text="Plain-text fallback. Please view the HTML version.",
            body_html=html,
            files=attachments,
            from_email=args.from_email,
            cc=args.cc,
            bcc=args.bcc,
            reply_to=args.reply_to,
            aws_region=args.aws_region,
            inline_images=inline_imgs,  # << attach PNG as CID
        )
        status = "OK" if res.get("ok") else "FAIL"
        logger.info("Send status=%s, message_id=%s, error=%s", status, res.get("message_id"), res.get("error"))
        print(json.dumps(res, indent=2))

def main():
    args = build_argparser().parse_args()
    asyncio.run(run(args))

if __name__ == "__main__":
    main()
