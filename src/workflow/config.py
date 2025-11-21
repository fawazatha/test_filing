from __future__ import annotations
from datetime import timedelta
from src.common.datetime import JAKARTA_TZ

# business constants
DIVIDEND_LOOKAHEAD_DAYS: int = 7

# Tag names
TAG_UPCOMING_DIVIDENDS = "upcoming-dividend"

TAG_INSIDER_BUY = "insider-1-month-buy"
TAG_INSIDER_SELL = "insider-1-month-sell"

TAG_LEADERS_1M = "top-ten-1m-leaders"
TAG_LAGGARDS_1M = "top-ten-1m-laggards"
TAG_LEADERS_1Y = "top-ten-1y-leaders"
TAG_LAGGARDS_1Y = "top-ten-1y-laggards"

TAG_TOP_90D_VOLUME = "top-90d-transaction-volume"
TAG_TOP_90D_VALUE = "top-90d-transaction-value"

TAG_TOP_INST_BUY = "top-20-institution-buying"
TAG_TOP_INST_SELL = "top-20-institution-selling"

TAG_NEW_HIGH = "new-high"
TAG_NEW_LOW = "new-low"


ALL_TAGS = {
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
}