# src/services/email/mailer.py
from __future__ import annotations

import os
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from .ses_email import send_attachments  # pakai fungsi yang sudah kamu buat


# -------- helpers --------
def _tolist(x: Optional[Sequence[str] | str]) -> List[str]:
    if x is None:
        return []
    if isinstance(x, str):
        return [s.strip() for s in x.split(",") if s.strip()]
    return [s for s in x if s]


def _esc(s: Any) -> str:
    return ("" if s is None else str(s)
            .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def _extract_primary_details(alert: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(alert.get("details"), dict):
        return alert["details"]

    reasons = alert.get("reasons") or []
    if not isinstance(reasons, list):
        return {}

    preferred_scopes = ("system", "tx")
    fallback_details: Optional[Dict[str, Any]] = None

    for r in reasons:
        if not isinstance(r, dict):
            continue
        scope = r.get("scope")
        d = r.get("details")
        if not isinstance(d, dict):
            continue
        if scope in preferred_scopes:
            return d
        if fallback_details is None:
            fallback_details = d

    return fallback_details or {}


def _flatten_alert_fields(a: Dict[str, Any]) -> Dict[str, Any]:
    details = _extract_primary_details(a)
    ctx = a.get("context") or {}
    if not isinstance(ctx, dict):
        ctx = {}
    doc = ctx.get("doc") or {}
    if not isinstance(doc, dict):
        doc = {}
    ann = a.get("announcement") or ctx.get("announcement") or {}
    if not isinstance(ann, dict):
        ann = {}

    sym = (
        a.get("symbol")
        or details.get("symbol")
        or a.get("ticker")
        or a.get("tickers")
        or ""
    )
    if isinstance(sym, list):
        sym = ",".join(str(s) for s in sym)

    holder = (
        a.get("holder_name")
        or details.get("holder_name")
        or a.get("holder")
        or a.get("name")
        or "-"
    )

    ttype = (
        a.get("type")
        or details.get("type")
        or a.get("transaction_type")
        or a.get("alert_type")
        or "-"
    )

    price = (
        a.get("price")
        or details.get("price")
        or a.get("parsed_price")
    )

    amount = (
        a.get("amount")
        or details.get("amount")
        or a.get("amount_transacted")
        or a.get("shares")
    )

    value = (
        a.get("value")
        or details.get("value")
        or a.get("transaction_value")
    )

    ts = (
        a.get("timestamp")
        or details.get("timestamp")
        or a.get("transaction_date")
        or details.get("transaction_date")
        or ann.get("published_at")
        or ann.get("publish_date")
        or a.get("date")
        or a.get("time")
        or "-"
    )

    src = (
        a.get("source")
        or doc.get("url")
        or doc.get("filename")
        or a.get("doc_url")
        or a.get("url")
        or ann.get("url")
        or ann.get("idx_url")
        or ann.get("link")
        or "-"
    )

    return {
        "symbol": sym,
        "holder": holder,
        "type": ttype,
        "price": price,
        "amount": amount,
        "value": value,
        "timestamp": ts,
        "source": src,
    }


def _render_email_content(alerts: List[Dict[str, Any]],
                          title: str = "IDX Alerts") -> tuple[str, str, str]:
    """
    Return: (subject, body_text, body_html)
    """
    n = len(alerts)
    today = datetime.now().strftime("%Y-%m-%d")
    subject = f"[{title}] {n} alert(s) — {today}"

    # plain text
    lines = [f"{title} — {n} alert(s) on {today}", "-" * 40]
    for i, a in enumerate(alerts, 1):
        flds = _flatten_alert_fields(a)

        sym = flds["symbol"]
        holder = flds["holder"]
        ttype = flds["type"]
        price = flds["price"]
        amount = flds["amount"]
        ts = flds["timestamp"]
        src = flds["source"]

        lines.append(
            f"{i}. {sym} | {ttype} | holder={holder} | amount={amount} | price={price} | ts={ts}"
        )
        lines.append(f"   src: {src}")

    body_text = "\n".join(lines)

    # html
    rows: List[str] = []
    for a in alerts:
        flds = _flatten_alert_fields(a)

        sym = flds["symbol"]
        holder = flds["holder"]
        ttype = flds["type"]
        price = flds["price"] or "-"
        amount = flds["amount"] or "-"
        value = flds["value"] or "-"
        ts = flds["timestamp"]
        src = flds["source"]

        link = (
            f'<a href="{_esc(src)}" target="_blank" rel="noopener">{_esc(src) or "-"}</a>'
            if src and src != "-"
            else "-"
        )

        rows.append(
            f"<tr>"
            f"<td>{_esc(ts)}</td>"
            f"<td><strong>{_esc(sym)}</strong></td>"
            f"<td>{_esc(holder)}</td>"
            f"<td>{_esc(ttype)}</td>"
            f"<td style='text-align:right'>{_esc(amount)}</td>"
            f"<td style='text-align:right'>{_esc(price)}</td>"
            f"<td style='text-align:right'>{_esc(value)}</td>"
            f"<td style='max-width:320px;overflow-wrap:anywhere'>{link}</td>"
            f"</tr>"
        )

    table = (
        "<table style='border-collapse:collapse;width:100%'>"
        "<thead>"
        "<tr style='background:#f3f4f6'>"
        "<th style='text-align:left;padding:8px;border:1px solid #e5e7eb'>Time</th>"
        "<th style='text-align:left;padding:8px;border:1px solid #e5e7eb'>Symbol</th>"
        "<th style='text-align:left;padding:8px;border:1px solid #e5e7eb'>Holder</th>"
        "<th style='text-align:left;padding:8px;border:1px solid #e5e7eb'>Type</th>"
        "<th style='text-align:right;padding:8px;border:1px solid #e5e7eb'>Amount</th>"
        "<th style='text-align:right;padding:8px;border:1px solid #e5e7eb'>Price</th>"
        "<th style='text-align:right;padding:8px;border:1px solid #e5e7eb'>Value</th>"
        "<th style='text-align:left;padding:8px;border:1px solid #e5e7eb'>Source</th>"
        "</tr>"
        "</thead>"
        "<tbody>" + "".join(rows) + "</tbody></table>"
    )

    body_html = (
        f"<div>"
        f"<h2 style='font-family:system-ui,Arial;margin:0 0 8px'>{_esc(title)}</h2>"
        f"<p style='margin:0 0 12px;color:#6b7280'>{n} alert(s) — {today}</p>"
        f"{table}"
        f"</div>"
    )

    return subject, body_text, body_html


def send_alerts_email(
    alerts: List[Dict[str, Any]],
    *,
    to: Optional[Sequence[str] | str] = None,
    cc: Optional[Sequence[str] | str] = None,
    bcc: Optional[Sequence[str] | str] = None,
    title: str = "IDX Alerts",
    attach_json_path: Optional[str] = None,
    aws_region: Optional[str] = None,
) -> dict:
    # determine recipients
    to_list = _tolist(to) or _tolist(os.getenv("ALERT_TO_EMAIL")) or _tolist(os.getenv("TEST_TO_EMAIL"))
    if not to_list:
        to_list = ["success@simulator.amazonses.com"]  # aman untuk sandbox

    subject, body_text, body_html = _render_email_content(alerts, title=title)

    # optional attachment
    files: List[str] = []
    path = attach_json_path
    if path is None:
        path = "./_tmp_alerts_preview.json"
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(alerts, f, ensure_ascii=False, indent=2)
        except Exception:
            path = None
    if path:
        files.append(path)

    return send_attachments(
        to=to_list,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
        files=files,
        cc=_tolist(cc),
        bcc=_tolist(bcc),
        aws_region=aws_region,
    )