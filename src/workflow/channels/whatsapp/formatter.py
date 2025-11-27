from __future__ import annotations

from collections import defaultdict
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
    TAG_TOP_INST_BUY,   
    TAG_TOP_INST_SELL,
    TAG_NEW_HIGH,
    TAG_NEW_LOW
)
from src.workflow.models import Workflow, WorkflowEvent


LOGGER = get_logger("workflow.whatsapp.formatter")

# Limit per section tags
MAX_ROWS_PER_SECTION = 5


# small helpers
def _abbr_num(x: Any) -> str:
    """
    Very small abbreviation helper for WhatsApp text (1.2K / 3.4M / 5.6B / 7.8T).
    """
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
def _section_upcoming_dividends(
    wf_name, 
    window_start, 
    window_end, 
    generated_at,
    events: List[WorkflowEvent], 
    sections: list
) -> list[dict[str, any]]:
    
    if not events:
        return sections

    for ev in _first(events):
        try:
            raw = ev.payload or {}

            div_per_share = raw.get("div_per_share") or raw.get("dividend_per_share") or raw.get("dividend")
            yield_pct = raw.get("yield") or raw.get("dividend_yield")
            pay_date = raw.get("payment_date") or raw.get("pay_date") or "-"
            ex_date = ev.payload.get("ex_date") or raw.get("ex_date") or raw.get("exDate") or "-"

            # Map to Template:
            # timeframe -> The Dates (Ex and Pay)
            # price -> The Money (DPS and Yield)
            time_str = f"Ex {ex_date}, Pay {pay_date}"
            price_str = f"DPS {_abbr_num(div_per_share)} ({_pct(yield_pct)})"

            sections.append(
                _make_template_item(
                    wf_name, 
                    window_start, 
                    window_end, 
                    generated_at, 
                    ev,
                    timeframe_text=time_str,
                    price_text=price_str
                )
            )
        
        except Exception as error: 
            LOGGER.error(f"Formatting failed for event dividend {ev.symbol}: {error}")
            continue

    return sections


def _section_insider(
    wf_name: str, 
    window_start: str, 
    window_end: str, 
    generated_at: str,
    insider_events: List[WorkflowEvent], 
    sections: list
) -> list[dict[str, any]]:
    
    if not insider_events:
        return sections

    for event in _first(insider_events):
        try:
            timeframe = 'BUY' if event.tag == TAG_INSIDER_BUY else 'SELL'

            sections.append(
                _make_template_item(
                    wf_name, 
                    window_start, 
                    window_end, 
                    generated_at, 
                    event,
                    timeframe_text=f"[{timeframe}]",
                    price_text=str(event.payload.get("value")) 
                )
            )
        
        except Exception as error: 
            LOGGER.error(f"Formatting failed for event insider {event.symbol}: {error}")
            continue
    
    return sections


def _section_institution(
    wf_name: str, 
    window_start: str, 
    window_end: str, 
    generated_at: str,
    institution_events: List[WorkflowEvent], 
    sections: list
) -> list[dict[str, any]]:
    if not institution_events:
        return sections

    for event in _first(institution_events): 
        try:
            raw = event.payload or {}
            timeframe = 'Top Buys' if event.tag == TAG_TOP_INST_BUY else 'Top Sells'

            sections.append(
                _make_template_item(
                    wf_name, 
                    window_start, 
                    window_end, 
                    generated_at, 
                    event,
                    timeframe_text=timeframe,
                    price_text=_abbr_num(raw.get("value"))
                )
            )
        
        except Exception as error:
            LOGGER.error(f"Formatting failed for event institution {event.symbol}: {error}")
            continue

    return sections


def _section_performance(
    wf_name: str, 
    window_start: str, 
    window_end: str, 
    generated_at: str,
    tag: str,
    event_list: List[WorkflowEvent], 
    sections: list
) -> list[dict[str, any]]:
    if not event_list:
        return sections

    for event in _first(event_list):
        try:
            raw = event.payload or {}
            timeframe = tag.replace("top-ten-", "").replace("-", " ").title()
            
            sections.append(
                _make_template_item(
                    wf_name, 
                    window_start, 
                    window_end, 
                    generated_at, 
                    event, 
                    timeframe_text=timeframe, 
                    price_text=str(raw.get("value"))
                )
            )
        
        except Exception as error:
            LOGGER.error(f"Formatting failed for event performance {event.symbol}: {error}")
            continue

    return sections


def _section_trading(
    wf_name: str,
    window_start: str,
    window_end: str,
    generated_at: str,
    sections: list,
    events: list,
    label: str,
    is_volume: bool = False
) -> list[dict[str, any]]:
    if not events:
        return sections 
    
    for event in _first(events):
        try:
            raw = event.payload or {}
            value = raw.get("value")

            if is_volume:
                price_text = f"{_abbr_num(value)} shares"
            else:
                price_text = _abbr_num(value)

            sections.append(
                _make_template_item(
                    wf_name,
                    window_start,
                    window_end,
                    generated_at,
                    event,
                    timeframe_text=label,
                    price_text=price_text,
                )
            )
        
        except Exception as error:
            LOGGER.error(f"Formatting failed for event trading {event.symbol}: {error}")
            continue

    return sections


def _section_high_low(
    wf_name: str,
    window_start: str,
    window_end: str,
    generated_at: str,
    high_low_events: List[WorkflowEvent], 
    sections: list
) -> list[dict[str, any]]:
    if not high_low_events:
        return sections

    for event in _first(high_low_events):
        try:
            raw = event.payload or {}
            raw_tf = raw.get("timeframe", "-")
            clean_tf = raw_tf.replace("_", " ").upper()

            sections.append(
                _make_template_item(
                    wf_name, 
                    window_start, 
                    window_end, 
                    generated_at, 
                    event,
                    timeframe_text=clean_tf,
                    price_text=_abbr_num(raw.get("price"))
                )
            )
        
        except Exception as error:
            LOGGER.error(f"Formatting failed for event high low {event.symbol}: {error}")
            continue

    return sections


def _make_template_item(
    workflow_name: str,
    period_start: str,
    period_end: str,
    generate_date: str,
    event: WorkflowEvent,
    timeframe_text: str,
    price_text: str
) -> dict[str, any]:
    """
    Standardizes the dictionary creation for the Twilio Template.
    """
    raw = event.payload or {}
    return {
        "workflow_name": workflow_name,
        "period_start": period_start,
        "period_end": period_end,
        "generate_date": generate_date,
        "symbol": event.symbol,
        "company_name": raw.get("company_name", "-"),
        "timeframe": timeframe_text,
        "price": price_text,
    }


# main entrypoint
def build_whatsapp_digest(
    workflow: Workflow,
    events: List[WorkflowEvent],
    ctx: Dict[str, Any],
) -> List[Dict[str, Any]] | None:
    """
    Build a list of WhatsApp template items for one workflow.

    ctx is expected to at least contain:
      - window_start (str)
      - window_end (str)
    """
    if not events:
        return None

    window_start = ctx.get("window_start")
    window_end = ctx.get("window_end")
    now = ctx.get("generated_at") or now_wib()
    generated_at = now.strftime('%Y-%m-%d %H:%M')
    wf_name = workflow.name or workflow.id

    # group events by tag for easier section-building
    by_tag: Dict[str, List[WorkflowEvent]] = defaultdict(list)
    for ev in events:
        by_tag[ev.tag].append(ev)

    sections: List[str] = []

    # upcoming dividends
    if TAG_UPCOMING_DIVIDENDS in workflow.tags:
        dividend_events = by_tag.get(TAG_UPCOMING_DIVIDENDS, [])
        sections = _section_upcoming_dividends(
            wf_name, window_start, window_end, generated_at, 
            dividend_events, sections
        )

    # insider trading (buy + sell)
    if TAG_INSIDER_BUY in workflow.tags or TAG_INSIDER_SELL in workflow.tags:
        insider_events = by_tag.get(TAG_INSIDER_BUY, []) + by_tag.get(TAG_INSIDER_SELL, [])
        sections = _section_insider(
            wf_name, window_start, window_end, generated_at, 
            insider_events, sections
        )

    # institution (only fires on day 11 per rules.py)
    if TAG_TOP_INST_BUY in workflow.tags or TAG_TOP_INST_SELL in workflow.tags:
        institution_events = by_tag.get(TAG_TOP_INST_BUY, []) + by_tag.get(TAG_TOP_INST_SELL, [])
        sections = _section_institution(
            wf_name, window_start, window_end, generated_at, 
            institution_events, sections
        )

    # performance (leaders/laggards 1m & 1y)
    performance_tags = [TAG_LEADERS_1M, TAG_LAGGARDS_1M, TAG_LEADERS_1Y, TAG_LAGGARDS_1Y]
    if any(t in workflow.tags for t in performance_tags):
        for tag in performance_tags:
            event_list = by_tag.get(tag, [])
            sections = _section_performance(
                wf_name, window_start, window_end, generated_at, tag,
                event_list, sections
            )

    # trading (90d volume & value)
    if TAG_TOP_90D_VOLUME in workflow.tags or TAG_TOP_90D_VALUE in workflow.tags:
        vol_events = by_tag.get(TAG_TOP_90D_VOLUME, [])
        val_events = by_tag.get(TAG_TOP_90D_VALUE, [])

        sections = _section_trading(
            wf_name, window_start, window_end, generated_at,
            sections,
            vol_events,
            label="Top Volume",
            is_volume=True
        )

        sections = _section_trading(
            wf_name, window_start, window_end, generated_at,
            sections,
            val_events,
            label="Top Value",
            is_volume=False
        )
        
    # high/low
    high_low_events = by_tag.get(TAG_NEW_HIGH, []) + by_tag.get(TAG_NEW_LOW, [])
    if high_low_events:
        sections = _section_high_low(
            wf_name, window_start, window_end, generated_at, 
            high_low_events, sections
        )

    if not sections:
        # all subscribed tags had no events in this window
        return None
    
    return sections
