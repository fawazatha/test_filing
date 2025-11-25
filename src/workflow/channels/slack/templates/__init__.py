from __future__ import annotations

from typing import Dict

from src.workflow.config import (
    TAG_UPCOMING_DIVIDENDS,
    TAG_LEADERS_1M,
    TAG_LAGGARDS_1M,
    TAG_LEADERS_1Y,
    TAG_LAGGARDS_1Y,
    TAG_TOP_90D_VOLUME,
    TAG_TOP_90D_VALUE,
    TAG_TOP_INST_BUY,
    TAG_TOP_INST_SELL,
)
from .base import SlackSectionTemplate
from .upcoming_dividends import UpcomingDividendsTemplate
from .ranking import RankingTemplate
from .insider_activity import render_insider_activity_section

# Instantiate templates once and reuse
TAG_TEMPLATES: Dict[str, SlackSectionTemplate] = {
    TAG_UPCOMING_DIVIDENDS: UpcomingDividendsTemplate(),
    TAG_LEADERS_1M: RankingTemplate(
        title="1-Month Performance Leaders",
        emoji="🚀",
        value_label="1M return",
    ),
    TAG_LAGGARDS_1M: RankingTemplate(
        title="1-Month Performance Laggards",
        emoji="📉",
        value_label="1M return",
    ),
    TAG_LEADERS_1Y: RankingTemplate(
        title="1-Year Performance Leaders",
        emoji="🚀",
        value_label="1Y return",
    ),
    TAG_LAGGARDS_1Y: RankingTemplate(
        title="1-Year Performance Laggards",
        emoji="📉",
        value_label="1Y return",
    ),
    TAG_TOP_90D_VOLUME: RankingTemplate(
        title="Top 90-Day Trading Volume",
        emoji="📊",
        value_label="volume",
    ),
    TAG_TOP_90D_VALUE: RankingTemplate(
        title="Top 90-Day Trading Value",
        emoji="💰",
        value_label="value",
    ),
    TAG_TOP_INST_BUY: RankingTemplate(
        title="Top Institutional Buys (Last Month)",
        emoji="🟢",
        value_label="value",
    ),
    TAG_TOP_INST_SELL: RankingTemplate(
        title="Top Institutional Sells (Last Month)",
        emoji="🔴",
        value_label="value",
    ),
}

__all__ = ["TAG_TEMPLATES", "render_insider_activity_section"]
