# src/workflow/channels/email/context.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from src.common.datetime import fmt_wib_date
from src.workflow.config import (
    TAG_UPCOMING_DIVIDENDS,
    TAG_INSIDER_BUY,
    TAG_INSIDER_SELL,
    TAG_LEADERS_1M,
    TAG_LAGGARDS_1M,
    TAG_LEADERS_1Y,
    TAG_LAGGARDS_1Y,
    TAG_TOP_90D_VOLUME,
    TAG_TOP_90D_VALUE,
    TAG_TOP_INST_BUY,
    TAG_TOP_INST_SELL,
)
from src.workflow.models import Workflow, WorkflowEvent


def build_email_context_from_events(
    workflow: Workflow,
    events: List[WorkflowEvent],
    window_start: datetime,
    window_end: datetime,
) -> Dict[str, Any]:
    """
    Build context dict untuk template Market Digest (workflow_template.html).

    Hanya menambahkan key untuk section yang memang ada datanya.
    Section yang didukung template:
      - dividends       → Upcoming Dividends
      - insider_rows    → Insider Trading Activity
      - inst_buys       → Institution Net Buying
      - inst_sells      → Institution Net Selling
      - leaders         → Top Leaders (1M / 1Y)
      - laggards        → Top Laggards (1M / 1Y)
      - top_volume      → Top 90D Transaction Volume
      - top_value       → Top 90D Transaction Value
    """

    ctx: Dict[str, Any] = {
        "window_start": fmt_wib_date(window_start),
        "window_end": fmt_wib_date(window_end),
        "as_of": fmt_wib_date(window_end),
        "period": "Sectors Daily Workflow",
    }

    dividends: List[Dict[str, Any]] = []
    insider_rows: List[Dict[str, Any]] = []
    inst_buys: List[Dict[str, Any]] = []
    inst_sells: List[Dict[str, Any]] = []
    leaders: List[Dict[str, Any]] = []
    laggards: List[Dict[str, Any]] = []
    top_volume: List[Dict[str, Any]] = []
    top_value: List[Dict[str, Any]] = []

    for ev in events:
        # Safety: jaga-jaga kalau ada events dari workflow lain
        if ev.workflow_id != workflow.id:
            continue

        tag = ev.tag
        p = ev.payload or {}

        # UPCOMING DIVIDENDS
        if tag == TAG_UPCOMING_DIVIDENDS:
            raw = p.get("raw") or {}
            company_name = (
                p.get("company_name")
                or raw.get("company_name")
                or raw.get("company")
                or ""
            )

            dividends.append(
                {
                    "ticker": ev.symbol,
                    "company": company_name,
                    # ex_date dari rules sudah dalam ISO date (YYYY-MM-DD)
                    "ex_date": p.get("ex_date"),
                    # div_share & yield dipetakan ke filter format_idr & % di template
                    "div_share": raw.get("div_share") or raw.get("dividend_per_share"),
                    "yield": raw.get("yield_pct"),
                    "pay_date": raw.get("payment_date"),
                }
            )

        # INSIDER TRADING (BUY / SELL)
        elif tag in (TAG_INSIDER_BUY, TAG_INSIDER_SELL):
            insider_rows.append(
                {
                    "symbol": ev.symbol,
                    "holder": p.get("holder_name") or p.get("holder"),
                    "type": "buy" if tag == TAG_INSIDER_BUY else "sell",
                    "price": p.get("price"),
                    # transaction_value / value akan di-format di template
                    "value": p.get("transaction_value") or p.get("value"),
                    "amount": p.get("amount"),
                    "pct_tx": p.get("share_percentage_transaction"),
                    # display_time sudah disiapkan di sisi rules / MV
                    "time": p.get("display_time"),
                }
            )

        # INSTITUTION ACTIVITY (TOP INST BUY/SELL)
        elif tag == TAG_TOP_INST_BUY:
            # Nilai 'value' di sini adalah total transaction value (IDR)
            inst_buys.append(
                {
                    "ticker": ev.symbol,
                    "value": p.get("value"),
                }
            )

        elif tag == TAG_TOP_INST_SELL:
            inst_sells.append(
                {
                    "ticker": ev.symbol,
                    "value": p.get("value"),
                }
            )

        # LEADERS / LAGGARDS (1M / 1Y)
        elif tag in (TAG_LEADERS_1M, TAG_LEADERS_1Y):
            leaders.append(
                {
                    "ticker": ev.symbol,
                    "return_1m": p.get("return_1m"),
                    "return_1y": p.get("return_1y"),
                    "source_tag": tag,
                }
            )

        elif tag in (TAG_LAGGARDS_1M, TAG_LAGGARDS_1Y):
            laggards.append(
                {
                    "ticker": ev.symbol,
                    "return_1m": p.get("return_1m"),
                    "return_1y": p.get("return_1y"),
                    "source_tag": tag,
                }
            )

        # TOP 90D VOLUME / VALUE
        elif tag == TAG_TOP_90D_VOLUME:
            top_volume.append(
                {
                    "ticker": ev.symbol,
                    # rules.py menyimpan angka di payload["value"], kita map ke "volume"
                    "volume": p.get("value"),
                    "rank": p.get("rank"),
                }
            )

        elif tag == TAG_TOP_90D_VALUE:
            top_value.append(
                {
                    "ticker": ev.symbol,
                    "value": p.get("value"),
                    "rank": p.get("rank"),
                }
            )

    if dividends:
        ctx["dividends"] = dividends

    if insider_rows:
        ctx["insider_rows"] = insider_rows

    if inst_buys:
        ctx["inst_buys"] = inst_buys

    if inst_sells:
        ctx["inst_sells"] = inst_sells

    if leaders:
        ctx["leaders"] = leaders

    if laggards:
        ctx["laggards"] = laggards

    if top_volume:
        ctx["top_volume"] = top_volume

    if top_value:
        ctx["top_value"] = top_value

    return ctx
