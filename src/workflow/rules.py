from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

from src.common.datetime import now_wib
from src.common.log import get_logger

from .config import (
    DIVIDEND_LOOKAHEAD_DAYS,
    TAG_UPCOMING_DIVIDENDS,
    TAG_TOP_INST_BUY,
    TAG_TOP_INST_SELL,
    TAG_NEW_HIGH,
    TAG_NEW_LOW,
)
from .models import TAG_TO_COLUMN, Workflow, WorkflowEvent

logger = get_logger("workflow.rules")

# High/low hierarchy:
HIGH_KEYS: List[Tuple[str, str]] = [
    ("alltime_high", "alltime_high"),
    ("yearly_high", "yearly_high"),
    ("ytd_high", "ytd_high"),
    ("quarterly_high", "quarterly_high"),
]

LOW_KEYS: List[Tuple[str, str]] = [
    ("alltime_low", "alltime_low"),
    ("yearly_low", "yearly_low"),
    ("ytd_low", "ytd_low"),
    ("quarterly_low", "quarterly_low"),
]


def _parse_json_array(value: Any) -> List[Dict[str, Any]]:
    """
    Robust parsing for JSON/JSONB or text JSON columns.
    Returns a list of dicts; if parsing fails or value is empty, returns [].
    """
    if not value:
        return []
    if isinstance(value, list):
        return value
    try:
        return json.loads(value)
    except Exception:
        return []


def _pick_first_non_null(row: Dict[str, Any], keys: List[str]) -> str | None:
    """
    Return the first key whose value in the row is not None.
    Used as a generic helper when we only care about which column is populated.
    """
    for k in keys:
        if row.get(k) is not None:
            return k
    return None


def _pick_high_low_value(
    row: Dict[str, Any], pairs: List[Tuple[str, str]]
) -> Tuple[float | None, str | None]:
    """
    Given a list of (column_name, timeframe_label) pairs,
    return (price, timeframe_label) for the first non-null column.

    Example:
      pairs = [("all_time_high_price", "all_time"), ("high_52w_price", "52w"), ...]
    """
    for col, timeframe in pairs:
        value = row.get(col)
        if value is None:
            continue
        try:
            # Try to cast to float; if it fails we still return raw value
            return float(value), timeframe
        except (TypeError, ValueError):
            return value, timeframe  # type: ignore[return-value]
    return None, None


# Regex for "value (rank)" patterns like "4.22 (1)" or "-3.5 (10)"
_CHANGE_RANK_RE = re.compile(r"\s*([+-]?\d+(?:\.\d+)?)\s*\((\d+)\)\s*$")


def parse_value_and_rank(raw: Any) -> tuple[Any | None, int | None]:
    """
    Parse simple "value (rank)" formats used in MV for leaders/laggards/etc.

    Supported patterns:
      - "4.22 (1)"      -> (4.22, 1)
      - "-3.5 (10)"     -> (-3.5, 10)
      - 4.22            -> (4.22, None)
      - None            -> (None, None)
      - "foo"           -> ("foo", None)   # if not numeric
    """
    if raw is None:
        return None, None
    if isinstance(raw, (int, float)):
        return float(raw), None

    s = str(raw)
    m = _CHANGE_RANK_RE.match(s)
    if m:
        val = float(m.group(1))
        rank = int(m.group(2))
        return val, rank

    # Fallback: try plain float
    try:
        return float(s), None
    except ValueError:
        return s, None


def build_events_for_row(
    workflow: Workflow,
    row: Dict[str, Any],
    now: datetime | None = None,
) -> List[WorkflowEvent]:
    """
    Given one MV row (one symbol) and one workflow, produce WorkflowEvent objects.

    All business rules are handled here:
      - upcoming_dividends: ex_date within [today, today + DIVIDEND_LOOKAHEAD_DAYS]
      - institution buy/sell: only send on the 11th
      - new-high / new-low: per-direction high/low hierarchy (all_time → 52w → ytd → 90d)
      - default tags: 1:1 MV column mapping via TAG_TO_COLUMN
    """
    now = now or now_wib()
    events: List[WorkflowEvent] = []

    symbol = row.get("symbol")
    company_name = row.get("company_name")

    for tag in workflow.tags:
        # 1) New High / New Low: single direction per tag with timeframe hierarchy
        if tag in (TAG_NEW_HIGH, TAG_NEW_LOW):
            if tag == TAG_NEW_HIGH:
                price, timeframe = _pick_high_low_value(row, HIGH_KEYS)
            else:
                price, timeframe = _pick_high_low_value(row, LOW_KEYS)

            if price is None or timeframe is None:
                continue

            events.append(
                WorkflowEvent(
                    workflow_id=workflow.id,
                    user_id=workflow.user_id,
                    symbol=symbol,
                    tag=tag,  # "new-high" or "new-low"
                    payload={
                        "timeframe": timeframe,      # "all_time" / "52w" / "ytd" / "90d"
                        "price": price,             # IDR per share (or raw value)
                        "company_name": company_name,
                    },
                )
            )
            continue

        # 2) Top institution buy/sell: only send on the 11th of the month
        if tag in (TAG_TOP_INST_BUY, TAG_TOP_INST_SELL):
            if now.day != 11:
                # Institution rankings are only announced once per month
                continue

            col = TAG_TO_COLUMN.get(tag)
            if not col:
                continue

            raw_value = row.get(col)
            if raw_value is None:
                continue

            value, rank = parse_value_and_rank(raw_value)

            events.append(
                WorkflowEvent(
                    workflow_id=workflow.id,
                    user_id=workflow.user_id,
                    symbol=symbol,
                    tag=tag,
                    payload={
                        "value": value,
                        "rank": rank,
                        "company_name": company_name,
                    },
                )
            )
            continue

        # 3) Upcoming dividends: ex_date within [today, today + DIVIDEND_LOOKAHEAD_DAYS]
        if tag == TAG_UPCOMING_DIVIDENDS:
            col = TAG_TO_COLUMN.get(tag)
            if not col:
                continue

            raw = row.get(col)
            items = _parse_json_array(raw)

            start = now.date()
            end = start + timedelta(days=DIVIDEND_LOOKAHEAD_DAYS)

            for item in items:
                ex_date_str = item.get("ex_date") or item.get("exDate")
                if not ex_date_str:
                    continue

                ex_date = None
                # Try a couple of common date/datetime formats
                for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
                    if ex_date:
                        break
                    try:
                        ex_date = datetime.strptime(ex_date_str[:19], fmt).date()
                    except Exception:
                        continue

                if not ex_date:
                    logger.warning("Cannot parse ex_date '%s' for %s", ex_date_str, symbol)
                    continue

                # Window logic: only notify when ex_date is within the lookahead window
                if not (start <= ex_date <= end):
                    continue

                days_to_ex = (ex_date - start).days

                events.append(
                    WorkflowEvent(
                        workflow_id=workflow.id,
                        user_id=workflow.user_id,
                        symbol=symbol,
                        tag=TAG_UPCOMING_DIVIDENDS,
                        payload={
                            "ex_date": ex_date.isoformat(),
                            "days_to_ex": days_to_ex,
                            "raw": item,  # full raw JSON object from MV
                            "company_name": company_name,
                        },
                    )
                )
            continue

        # 4) Default: 1:1 mapping to a single MV column via TAG_TO_COLUMN
        col = TAG_TO_COLUMN.get(tag)
        if not col:
            # Unknown or unmapped tag, skip silently
            continue

        raw_value = row.get(col)
        if raw_value is None:
            continue
        if isinstance(raw_value, str) and not raw_value.strip():
            continue
        if isinstance(raw_value, list) and len(raw_value) == 0:
            continue

        value, rank = parse_value_and_rank(raw_value)

        events.append(
            WorkflowEvent(
                workflow_id=workflow.id,
                user_id=workflow.user_id,
                symbol=symbol,
                tag=tag,
                payload={
                    "value": value,
                    "rank": rank,
                    "company_name": company_name,
                },
            )
        )

    return events
