from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Dict, Any

TxnType = Literal["buy", "sell", "transfer"]

class Transaction(BaseModel):
    type: TxnType
    price: float = 0
    amount: int = 0
    value: float = 0
    holding_before: Optional[int] = None
    holding_after: Optional[int] = None
    share_percentage_before: Optional[float] = None
    share_percentage_after: Optional[float] = None
    share_percentage_transaction: Optional[float] = None

class LinkingMeta(BaseModel):
    uid: Optional[str] = None
    paired_symbol: Optional[str] = None
    paired_filing_id: Optional[str] = None
    is_share_transfer: bool = False
    is_intercorporate_cash: bool = False
    intercorporate_tag: Optional[Literal["intercorporate-buy","intercorporate-sell"]] = None
    show_on_company_page: bool = True
    reason_hidden: Optional[Literal["symbol_equals_holder_name","not_affected","unknown"]] = None

class Filing(BaseModel):
    source: str
    symbol: str
    date: str
    holder_name: str
    holder_type: Optional[str] = None

    holding_before: Optional[int] = 0
    holding_after: Optional[int] = 0
    share_percentage_before: Optional[float] = 0.0
    share_percentage_after: Optional[float] = 0.0

    transactions: List[Transaction] = Field(default_factory=list)
    transaction_value: float = 0

    sector: Optional[str] = None
    subsector: Optional[str] = None

    link: LinkingMeta = Field(default_factory=LinkingMeta)
    extra: Dict[str, Any] = Field(default_factory=dict)
