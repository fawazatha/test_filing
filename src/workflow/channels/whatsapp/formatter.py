from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Iterable

from src.common.datetime import now_wib
from src.common.log import get_logger
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
    TAG_INST_BUY,
    TAG_INST_SELL,
    TAG_HIGH_LOW,
)
from src.workflow.models import Workflow, WorkflowEvent

logger = get_logger("workflow.whatsapp.formatter")

# Limit per section supaya pesan WA nggak jadi tembok text
MAX_ROWS_PER_SECTION = 5


# small helpers

def _abbr_num(x: Any) -> str:
    """Very small abbreviation helper for WhatsApp text (1.2K / 3.4M / 5.6B / 7.8T)."""
    if x is None:
        return "-"
    try:
        n = float(x)
    except Exception:
        return str(x)

    abs_n = abs(n)
    if abs_n >= 1_000_000_000_000:
        return f"{n/1_000_000_000_000:.1f}T"
    if abs_n >= 1_000_000_000:
        return f"{n/1_000_000_000:.1f}B"
    if abs_n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if abs_n >= 1_000:
        return f"{n/1_000:.1f}K"
    return f"{n:.0f}"


def _pct(x: Any) -> str:
    try:
        return f"{float(x):.1f}%"
    except Exception:
        return str(x)


def _first(items: Iterable[Any], limit: int = MAX_ROWS_PER_SECTION) -> List[Any]:
    out: List[Any] = []
    for it in items:
        if len(out) >= limit:
            break
        out.append(it)
    return out


# per-tag section builders
def _section_upcoming_dividends(events: List[WorkflowEvent]) -> str:
    if not events:
        return ""

    lines = ["📈 *Upcoming Dividends*"]
    # Each event payload: {"ex_date": "...", "raw": {...}}
    for ev in _first(events):
        raw = ev.payload.get("raw") or {}
        company = raw.get("company_name") or raw.get("company") or "-"
        div_per_share = raw.get("div_per_share") or raw.get("dividend_per_share") or raw.get("dividend") or "-"
        yield_pct = raw.get("yield") or raw.get("dividend_yield") or "-"
        pay_date = raw.get("payment_date") or raw.get("pay_date") or "-"
        ex_date = ev.payload.get("ex_date") or raw.get("ex_date") or raw.get("exDate") or "-"

        lines.append(
            f"- {ev.symbol} {company}: "
            f"DPS {_abbr_num(div_per_share)} "
            f"({_pct(yield_pct)}), Ex {ex_date}, Pay {pay_date}"
        )

    if len(events) > MAX_ROWS_PER_SECTION:
        lines.append(f"… and {len(events) - MAX_ROWS_PER_SECTION} more")

    return "\n".join(lines)


def _section_insider(events_buy: List[WorkflowEvent], events_sell: List[WorkflowEvent]) -> str:
    if not events_buy and not events_sell:
        return ""

    lines: List[str] = ["🔍 *Insider Trading Activity*"]

    def _format(ev: WorkflowEvent, label: str) -> str:
        # For now we rely on payload["value"] summarizing the move (e.g. '12.5B (rank 1)')
        value = ev.payload.get("value")
        company = ev.payload.get("company_name") or "-"
        return f"- [{label}] {ev.symbol} {company}: {value}"

    if events_buy:
        lines.append("  • BUY:")
        for ev in _first(events_buy):
            lines.append("    " + _format(ev, "BUY"))
        if len(events_buy) > MAX_ROWS_PER_SECTION:
            lines.append(f"    … and {len(events_buy) - MAX_ROWS_PER_SECTION} more")

    if events_sell:
        lines.append("  • SELL:")
        for ev in _first(events_sell):
            lines.append("    " + _format(ev, "SELL"))
        if len(events_sell) > MAX_ROWS_PER_SECTION:
            lines.append(f"    … and {len(events_sell) - MAX_ROWS_PER_SECTION} more")

    return "\n".join(lines)


def _section_institution(events_buy: List[WorkflowEvent], events_sell: List[WorkflowEvent]) -> str:
    if not events_buy and not events_sell:
        return ""

    lines: List[str] = ["🏢 *Institution Activity (Last Month)*"]

    if events_buy:
        lines.append("  • Top Buys:")
        for ev in _first(events_buy):
            value = ev.payload.get("value")
            company = ev.payload.get("company_name") or "-"
            lines.append(f"    - {ev.symbol} {company}: {_abbr_num(value)}")
        if len(events_buy) > MAX_ROWS_PER_SECTION:
            lines.append(f"    … and {len(events_buy) - MAX_ROWS_PER_SECTION} more")

    if events_sell:
        lines.append("  • Top Sells:")
        for ev in _first(events_sell):
            value = ev.payload.get("value")
            company = ev.payload.get("company_name") or "-"
            lines.append(f"    - {ev.symbol} {company}: {_abbr_num(value)}")
        if len(events_sell) > MAX_ROWS_PER_SECTION:
            lines.append(f"    … and {len(events_sell) - MAX_ROWS_PER_SECTION} more")

    return "\n".join(lines)


def _section_performance(
    leaders_1m: List[WorkflowEvent],
    laggards_1m: List[WorkflowEvent],
    leaders_1y: List[WorkflowEvent],
    laggards_1y: List[WorkflowEvent],
) -> str:
    if not (leaders_1m or laggards_1m or leaders_1y or laggards_1y):
        return ""

    lines: List[str] = ["📊 *Performance Leaders & Laggards*"]

    def _fmt(ev: WorkflowEvent) -> str:
        # payload["value"] is typically "4.2 (1)" or similar (return + rank)
        value = ev.payload.get("value")
        company = ev.payload.get("company_name") or "-"
        return f"- {ev.symbol} {company}: {value}"

    if leaders_1m:
        lines.append("  • 1M Leaders:")
        for ev in _first(leaders_1m):
            lines.append("    " + _fmt(ev))
        if len(leaders_1m) > MAX_ROWS_PER_SECTION:
            lines.append(f"    … and {len(leaders_1m) - MAX_ROWS_PER_SECTION} more")

    if laggards_1m:
        lines.append("  • 1M Laggards:")
        for ev in _first(laggards_1m):
            lines.append("    " + _fmt(ev))
        if len(laggards_1m) > MAX_ROWS_PER_SECTION:
            lines.append(f"    … and {len(laggards_1m) - MAX_ROWS_PER_SECTION} more")

    if leaders_1y:
        lines.append("  • 1Y Leaders:")
        for ev in _first(leaders_1y):
            lines.append("    " + _fmt(ev))
        if len(leaders_1y) > MAX_ROWS_PER_SECTION:
            lines.append(f"    … and {len(leaders_1y) - MAX_ROWS_PER_SECTION} more")

    if laggards_1y:
        lines.append("  • 1Y Laggards:")
        for ev in _first(laggards_1y):
            lines.append("    " + _fmt(ev))
        if len(laggards_1y) > MAX_ROWS_PER_SECTION:
            lines.append(f"    … and {len(laggards_1y) - MAX_ROWS_PER_SECTION} more")

    return "\n".join(lines)


def _section_trading(volume_events: List[WorkflowEvent], value_events: List[WorkflowEvent]) -> str:
    if not (volume_events or value_events):
        return ""

    lines: List[str] = ["💹 *Trading Activity (90D)*"]

    if volume_events:
        lines.append("  • Top Volume:")
        for ev in _first(volume_events):
            company = ev.payload.get("company_name") or "-"
            vol = ev.payload.get("value")
            lines.append(f"    - {ev.symbol} {company}: {_abbr_num(vol)} shares")
        if len(volume_events) > MAX_ROWS_PER_SECTION:
            lines.append(f"    … and {len(volume_events) - MAX_ROWS_PER_SECTION} more")

    if value_events:
        lines.append("  • Top Value:")
        for ev in _first(value_events):
            company = ev.payload.get("company_name") or "-"
            val = ev.payload.get("value")
            lines.append(f"    - {ev.symbol} {company}: {_abbr_num(val)}")
        if len(value_events) > MAX_ROWS_PER_SECTION:
            lines.append(f"    … and {len(value_events) - MAX_ROWS_PER_SECTION} more")

    return "\n".join(lines)


def _section_high_low(events: List[WorkflowEvent]) -> str:
    if not events:
        return ""

    lines = ["📌 *High / Low Highlights*"]
    for ev in _first(events):
        company = ev.payload.get("company_name") or "-"
        timeframe = ev.payload.get("timeframe")  # e.g. "ytd_high", "alltime_low"
        price = ev.payload.get("price")
        tf = (timeframe or "").replace("_", " ").upper()
        lines.append(
            f"- {ev.symbol} {company}: ({tf}) at {_abbr_num(price)}"
        )

    if len(events) > MAX_ROWS_PER_SECTION:
        lines.append(f"… and {len(events) - MAX_ROWS_PER_SECTION} more")

    return "\n".join(lines)


# main entrypoint

def build_whatsapp_digest(
    workflow: Workflow,
    events: List[WorkflowEvent],
    ctx: Dict[str, Any],
) -> str | None:
    """
    Build a single WhatsApp message body for one workflow.

    ctx is expected to at least contain:
      - window_start (str)
      - window_end (str)
    """
    if not events:
        return None

    window_start = ctx.get("window_start")
    window_end = ctx.get("window_end")
    now = ctx.get("generated_at") or now_wib()

    header_lines = [
        f"*Sectors Market Digest* – {workflow.name or workflow.id}",
        f"Period: {window_start} → {window_end} (WIB)",
        f"Generated at: {now.strftime('%Y-%m-%d %H:%M')} WIB",
        "",
        "Summary for your subscribed tags:",
    ]

    # group events by tag for easier section-building
    by_tag: Dict[str, List[WorkflowEvent]] = defaultdict(list)
    for ev in events:
        by_tag[ev.tag].append(ev)

    sections: List[str] = []

    # upcoming dividends
    if TAG_UPCOMING_DIVIDENDS in workflow.tags:
        sec = _section_upcoming_dividends(by_tag.get(TAG_UPCOMING_DIVIDENDS, []))
        if sec:
            sections.append(sec)

    # insider trading (buy + sell)
    if TAG_INSIDER_BUY in workflow.tags or TAG_INSIDER_SELL in workflow.tags:
        sec = _section_insider(
            by_tag.get(TAG_INSIDER_BUY, []),
            by_tag.get(TAG_INSIDER_SELL, []),
        )
        if sec:
            sections.append(sec)

    # institution (only fires on day 11 per rules.py)
    if TAG_INST_BUY in workflow.tags or TAG_INST_SELL in workflow.tags:
        sec = _section_institution(
            by_tag.get(TAG_INST_BUY, []),
            by_tag.get(TAG_INST_SELL, []),
        )
        if sec:
            sections.append(sec)

    # performance (leaders/laggards 1m & 1y)
    if any(t in workflow.tags for t in (TAG_LEADERS_1M, TAG_LAGGARDS_1M, TAG_LEADERS_1Y, TAG_LAGGARDS_1Y)):
        sec = _section_performance(
            by_tag.get(TAG_LEADERS_1M, []),
            by_tag.get(TAG_LAGGARDS_1M, []),
            by_tag.get(TAG_LEADERS_1Y, []),
            by_tag.get(TAG_LAGGARDS_1Y, []),
        )
        if sec:
            sections.append(sec)

    # trading (90d volume & value)
    if TAG_TOP_90D_VOLUME in workflow.tags or TAG_TOP_90D_VALUE in workflow.tags:
        sec = _section_trading(
            by_tag.get(TAG_TOP_90D_VOLUME, []),
            by_tag.get(TAG_TOP_90D_VALUE, []),
        )
        if sec:
            sections.append(sec)

    # high/low
    if TAG_HIGH_LOW in workflow.tags:
        sec = _section_high_low(by_tag.get(TAG_HIGH_LOW, []))
        if sec:
            sections.append(sec)

    if not sections:
        # all subscribed tags had no events in this window
        return None

    body = "\n\n".join(header_lines + sections)
    return body
