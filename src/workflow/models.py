from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Any

from .config import (
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

TAG_TO_COLUMN = {
    TAG_UPCOMING_DIVIDENDS: "upcoming_dividends",
    TAG_INSIDER_BUY: "idx_filings_buy",
    TAG_INSIDER_SELL: "idx_filings_sell",
    TAG_LEADERS_1M: "one_month_leaders",
    TAG_LAGGARDS_1M: "one_month_laggards",
    TAG_LEADERS_1Y: "one_year_leaders",
    TAG_LAGGARDS_1Y: "one_year_laggards",
    TAG_TOP_90D_VOLUME: "top_3m_volume_transaction",
    TAG_TOP_90D_VALUE: "top_3m_value_transaction",
    TAG_TOP_INST_BUY: "last_month_top_institution_transaction_bought",
    TAG_TOP_INST_SELL: "last_month_top_institution_transaction_sold",
}


@dataclass
class Workflow:
    id: str
    user_id: int
    name: str
    tickers: List[str]
    tags: List[str]
    sectors: List[str]
    sub_sectors: List[str]
    industries: List[str]
    sub_industries: List[str]
    channels: Dict[str, Any]
    is_active: bool


@dataclass
class WorkflowEvent:
    workflow_id: str
    user_id: int
    symbol: str
    tag: str
    payload: Dict[str, Any]
