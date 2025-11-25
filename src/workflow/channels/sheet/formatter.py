# src/workflow/channels/sheet/formatter.py
from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, List
import json

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
    TAG_NEW_LOW,
)
from src.workflow.models import WorkflowEvent
from src.common.datetime import now_wib

# ---------------------------------------------------------------------------
# Sheet configuration
# ---------------------------------------------------------------------------

SHEET_CONFIG: Dict[str, Dict[str, Any]] = {
    TAG_UPCOMING_DIVIDENDS: {
        "sheet_name": "Upcoming Dividends",
        "headers": [
            "Date",
            "Ticker",
            "Company",
            "Ex-Date",
            "Div/Share (IDR)",
            "Yield (%)",
            "Pay-Date",
            "Source",
        ],
    },
    TAG_INSIDER_BUY: {
        "sheet_name": "Insider BUY",
        "headers": [
            "Date",
            "Ticker",
            "Company",
            "Holder",
            "Type",              # BUY/SELL
            "Price (IDR)",
            "Value (IDR)",
            "Amount",
            "% Tx",
            "Time",
            "Source",
        ],
    },
    TAG_INSIDER_SELL: {
        "sheet_name": "Insider SELL",
        "headers": [
            "Date",
            "Ticker",
            "Company",
            "Holder",
            "Type",
            "Price (IDR)",
            "Value (IDR)",
            "Amount",
            "% Tx",
            "Time",
            "Source",
        ],
    },
    TAG_NEW_HIGH: {
        "sheet_name": "New High Levels",
        "headers": [
            "Date",
            "Ticker",
            "Company",
            "Timeframe",
            "Price (IDR)",
            "Notes",
        ],
    },
    TAG_NEW_LOW: {
        "sheet_name": "New Low Levels",
        "headers": [
            "Date",
            "Ticker",
            "Company",
            "Timeframe",
            "Price (IDR)",
            "Notes",
        ],
    },

    TAG_LEADERS_1M: {
        "sheet_name": "Leaders 1M",
        "headers": [
            "Date",
            "Ticker",
            "Company",
            "1M Return (%)",
            "1Y Return (%)",
            "Rank",
        ],
    },
    TAG_LAGGARDS_1M: {
        "sheet_name": "Laggards 1M",
        "headers": [
            "Date",
            "Ticker",
            "Company",
            "1M Return (%)",
            "1Y Return (%)",
            "Rank",
        ],
    },
    TAG_LEADERS_1Y: {
        "sheet_name": "Leaders 1Y",
        "headers": [
            "Date",
            "Ticker",
            "Company",
            "1Y Return (%)",
            "1M Return (%)",
            "Rank",
        ],
    },
    TAG_LAGGARDS_1Y: {
        "sheet_name": "Laggards 1Y",
        "headers": [
            "Date",
            "Ticker",
            "Company",
            "1Y Return (%)",
            "1M Return (%)",
            "Rank",
        ],
    },
    TAG_TOP_90D_VOLUME: {
        "sheet_name": "Top 90D Volume",
        "headers": [
            "Date",
            "Ticker",
            "Company",
            "90D Volume (shares)",
            "Rank",
        ],
    },
    TAG_TOP_90D_VALUE: {
        "sheet_name": "Top 90D Value",
        "headers": [
            "Date",
            "Ticker",
            "Company",
            "90D Value (IDR)",
            "Rank",
        ],
    },
    TAG_TOP_INST_BUY: {
        "sheet_name": "Top Inst Buys",
        "headers": [
            "Date",
            "Ticker",
            "Company",
            "Last Month Value (IDR)",
            "Rank",
        ],
    },
    TAG_TOP_INST_SELL: {
        "sheet_name": "Top Inst Sells",
        "headers": [
            "Date",
            "Ticker",
            "Company",
            "Last Month Value (IDR)",
            "Rank",
        ],
    },
}


def _safe_json_array(value: Any) -> List[Dict[str, Any]]:
    """Best-effort parse JSON/text/list into a list of dicts."""
    if not value:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            return []
    return []


def _friendly_timeframe(tf_key: str) -> str:
    """
    Map MV keys to friendly timeframe labels.
    e.g. 'alltime_high' -> 'All-time', 'yearly_low' -> '52w'
    """
    tf_key = (tf_key or "").lower()
    if "alltime" in tf_key:
        return "All-time"
    if "yearly" in tf_key or "52w" in tf_key:
        return "52w"
    if "quarterly" in tf_key or "90d" in tf_key:
        return "90D"
    if "ytd" in tf_key:
        return "YTD"
    return tf_key


def build_sheet_blocks(
    events: List[WorkflowEvent],
    as_of: datetime | None = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Take all WorkflowEvent (for a single run/batch) and return:

      {
        tag: {
          "sheet_name": str,
          "headers": [...],
          "rows": [ [...], [...] ]
        },
        ...
      }

    Sender will simply append rows to the correct sheet for each tag.
    """
    as_of = as_of or now_wib()
    date_str = as_of.date().isoformat()

    blocks: Dict[str, Dict[str, Any]] = {}

    for ev in events:
        cfg = SHEET_CONFIG.get(ev.tag)
        if not cfg:
            # Tag not mapped for sheet, skip
            continue

        tag = ev.tag
        if tag not in blocks:
            blocks[tag] = {
                "sheet_name": cfg["sheet_name"],
                "headers": cfg["headers"],
                "rows": [],
            }

        rows = blocks[tag]["rows"]
        payload = ev.payload or {}
        company = payload.get("company_name") or payload.get("company") or ""

        # === Upcoming Dividends ===
        if tag == TAG_UPCOMING_DIVIDENDS:
            raw = payload.get("raw") or {}
            ex_date = payload.get("ex_date") or raw.get("ex_date")

            div_share = (
                raw.get("div_share")
                or raw.get("dividend_per_share")
                or raw.get("dividend_amount")
            )
            yd = raw.get("yield_pct") or raw.get("yield")
            pay_date = raw.get("payment_date") or raw.get("pay_date")
            source = raw.get("source") or raw.get("url") or ""

            rows.append(
                [
                    date_str,
                    ev.symbol,
                    company,
                    ex_date or "",
                    div_share if div_share is not None else "",
                    yd if yd is not None else "",
                    pay_date or "",
                    source,
                ]
            )
            continue

        # === Insider trading (buy/sell) ===
        if tag in (TAG_INSIDER_BUY, TAG_INSIDER_SELL):
            filings = _safe_json_array(payload.get("value"))
            for f in filings:
                holder = f.get("holder_name") or f.get("holder") or ""
                tx_type = (
                    f.get("transaction_type")
                    or f.get("type")
                    or ("buy" if tag == TAG_INSIDER_BUY else "sell")
                )
                price = f.get("price") or f.get("avg_price") or ""
                value = f.get("transaction_value") or f.get("value") or ""
                amount = f.get("amount") or f.get("shares") or ""
                pct_tx = f.get("share_percentage_transaction") or f.get("pct_tx") or ""
                time = f.get("display_time") or f.get("time") or ""
                src = f.get("source") or f.get("url") or ""

                comp = (
                    f.get("company_name")
                    or f.get("company")
                    or company
                )

                rows.append(
                    [
                        date_str,
                        ev.symbol,
                        comp,
                        holder,
                        str(tx_type).upper(),
                        price,
                        value,
                        amount,
                        pct_tx,
                        time,
                        src,
                    ]
                )
            continue

        if tag in (TAG_NEW_HIGH, TAG_NEW_LOW):
            timeframe_key = (
                payload.get("timeframe")
                or payload.get("timeframe_key")
                or ""
            )
            tf_label = _friendly_timeframe(timeframe_key)
            price = payload.get("price", "")

            rows.append(
                [
                    date_str,
                    ev.symbol,
                    company,
                    tf_label,
                    price,
                    timeframe_key,  # raw key in Notes for debugging
                ]
            )
            continue

        # === Generic metric + rank tags ===
        metric = payload.get("value")
        rank = payload.get("rank")
        metric_str = "" if metric is None else str(metric)
        rank_str = "" if rank is None else str(rank)

        if tag in (TAG_LEADERS_1M, TAG_LAGGARDS_1M):
            rows.append(
                [
                    date_str,
                    ev.symbol,
                    company,
                    metric_str,  # 1M return
                    "",          # 1Y return (optional, can be filled later)
                    rank_str,
                ]
            )
            continue

        if tag in (TAG_LEADERS_1Y, TAG_LAGGARDS_1Y):
            rows.append(
                [
                    date_str,
                    ev.symbol,
                    company,
                    metric_str,  # 1Y return
                    "",          # 1M return (optional, can be filled later)
                    rank_str,
                ]
            )
            continue

        if tag == TAG_TOP_90D_VOLUME:
            rows.append(
                [
                    date_str,
                    ev.symbol,
                    company,
                    metric_str,  # 90D volume
                    rank_str,
                ]
            )
            continue

        if tag == TAG_TOP_90D_VALUE:
            rows.append(
                [
                    date_str,
                    ev.symbol,
                    company,
                    metric_str,  # 90D value
                    rank_str,
                ]
            )
            continue

        if tag in (TAG_TOP_INST_BUY, TAG_TOP_INST_SELL):
            rows.append(
                [
                    date_str,
                    ev.symbol,
                    company,
                    metric_str,  # last month value
                    rank_str,
                ]
            )
            continue

    return blocks
