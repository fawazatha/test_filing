from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List

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
    TAG_TOP_INST_BUY,
    TAG_TOP_INST_SELL,
)
from src.workflow.models import Workflow, WorkflowEvent
from src.workflow.channels.slack.templates import TAG_TEMPLATES, render_insider_activity_section

logger = get_logger("workflow.slack")


def _group_events_by_tag(events: Iterable[WorkflowEvent]) -> Dict[str, List[WorkflowEvent]]:
    grouped: Dict[str, List[WorkflowEvent]] = defaultdict(list)
    for ev in events:
        grouped[ev.tag].append(ev)
    return grouped


def _build_quick_takeaways(by_tag: Dict[str, List[WorkflowEvent]]) -> str:
    """
    Build a small 'Quick takeaways' section at the top, based on available tags.
    Keep it simple; can be upgraded later or replaced by LLM-based summaries.
    """
    bullets: List[str] = []

    leaders_1m = by_tag.get(TAG_LEADERS_1M) or []
    laggards_1m = by_tag.get(TAG_LAGGARDS_1M) or []
    inst_buys = by_tag.get(TAG_TOP_INST_BUY) or []
    inst_sells = by_tag.get(TAG_TOP_INST_SELL) or []
    insider_buys = by_tag.get(TAG_INSIDER_BUY) or []
    insider_sells = by_tag.get(TAG_INSIDER_SELL) or []

    if leaders_1m:
        ev = leaders_1m[0]
        val = (ev.payload or {}).get("value")
        bullets.append(f"• Top 1M leader in your universe: `{ev.symbol}` ({val} 1M return).")

    if laggards_1m:
        ev = laggards_1m[0]
        val = (ev.payload or {}).get("value")
        bullets.append(f"• Biggest 1M laggard: `{ev.symbol}` ({val} 1M return).")

    if insider_buys or insider_sells:
        bullets.append(
            f"• Insider activity this window: {len(insider_buys)} BUY and {len(insider_sells)} SELL event(s)."
        )

    if inst_buys or inst_sells:
        bullets.append(
            f"• Institutional flows snapshot: {len(inst_buys)} top buys and {len(inst_sells)} top sells recorded."
        )

    if not bullets:
        return ""

    lines = ["*💡 Quick takeaways*"]
    lines.extend(bullets)
    return "\n".join(lines)


def build_slack_payload_for_workflow(
    *,
    workflow: Workflow,
    events: List[WorkflowEvent],
    window_start: str,
    window_end: str,
) -> Dict[str, Any]:
    """
    Build 1 Slack message payload (for webhook) per workflow.

    Style:
      - Header with "Market Sector Digest" + period
      - Optional 'Quick takeaways'
      - Per-tag narrative sections (using modular templates)
    Output:
      Simple Block Kit: a single mrkdwn section for easy integration.
    """
    if not events:
        text = (
            f":bar_chart: *Market Digest*\n"
            f"_Workflow:_ *{workflow.name}*\n"
            f"*Period:* {window_start} → {window_end} (WIB)\n\n"
            "_No data for your subscribed tags in this window._"
        )
        return {
            "text": text,
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": text}},
            ],
        }

    by_tag = _group_events_by_tag(events)

    # Header
    header_lines: List[str] = []
    header_lines.append(":bar_chart: *Market Sector Digest*")
    header_lines.append(f"*Period:* {window_start} → {window_end} (WIB)")

    section_chunks: List[str] = []

    # Quick takeaways
    quick = _build_quick_takeaways(by_tag)
    if quick:
        section_chunks.append(quick)

    # Upcoming dividends
    if TAG_UPCOMING_DIVIDENDS in by_tag:
        tmpl = TAG_TEMPLATES.get(TAG_UPCOMING_DIVIDENDS)
        if tmpl:
            s = tmpl.render(
                workflow=workflow,
                events=by_tag[TAG_UPCOMING_DIVIDENDS],
                window_start=window_start,
                window_end=window_end,
            )
            if s.strip():
                section_chunks.append(s)

    # Insider activity (buy + sell combined)
    buy_events = by_tag.get(TAG_INSIDER_BUY, [])
    sell_events = by_tag.get(TAG_INSIDER_SELL, [])
    if buy_events or sell_events:
        s = render_insider_activity_section(
            workflow=workflow,
            buy_events=buy_events,
            sell_events=sell_events,
            window_start=window_start,
            window_end=window_end,
        )
        if s.strip():
            section_chunks.append(s)

    # Ranking sections – use the registry so we don't hardcode copy here
    for tag in [
        TAG_LEADERS_1M,
        TAG_LAGGARDS_1M,
        TAG_LEADERS_1Y,
        TAG_LAGGARDS_1Y,
        TAG_TOP_90D_VOLUME,
        TAG_TOP_90D_VALUE,
        TAG_TOP_INST_BUY,
        TAG_TOP_INST_SELL,
    ]:
        if tag not in by_tag:
            continue
        tmpl = TAG_TEMPLATES.get(tag)
        if not tmpl:
            continue
        s = tmpl.render(
            workflow=workflow,
            events=by_tag[tag],
            window_start=window_start,
            window_end=window_end,
        )
        if s.strip():
            section_chunks.append(s)

    if not section_chunks:
        body_text = "_No visible data for your subscribed tags in this window._"
    else:
        body_text = "\n\n".join(section_chunks)

    # Workflow name as small footer (since name may be user-defined / misleading)
    footer = f"_Workflow: {workflow.name or 'Untitled Workflow'}_"

    full_text = "\n".join(header_lines) + "\n\n" + body_text + "\n\n" + footer

    return {
        "text": full_text,
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": full_text,
                },
            }
        ],
    }
