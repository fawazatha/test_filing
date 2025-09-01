from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List, Optional
from datetime import datetime

@dataclass
class PriceTransactionData:
    """Represents price and transaction amount data."""
    prices: List[int] = field(default_factory=list)
    amount_transacted: List[int] = field(default_factory=list)


@dataclass
class FilingInfo:
    """
    Represents extracted filing information.
    
    This class holds all the structured data extracted from raw filing text.
    """
    document_number: str = ""
    company_name: str = ""
    holder_name: str = ""
    ticker: str = ""
    category: str = ""
    control_status: str = ""
    purpose: str = ""
    date_time: str = ""
    
    # Financial data
    shareholding_before: str = ""
    shareholding_after: str = ""
    share_percentage_before: str = ""
    share_percentage_after: str = ""
    share_percentage_transaction: str = ""
    
    # Transaction data
    price_transaction: PriceTransactionData = field(default_factory=PriceTransactionData)


