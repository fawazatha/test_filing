# src/core/types.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

FILINGS_ALLOWED_COLUMNS = {
    "symbol", "timestamp", "transaction_type", "holder_name",
    "holding_before", "holding_after", "amount_transaction",
    "share_percentage_before", "share_percentage_after", "share_percentage_transaction",
    "price", "transaction_value",
    "price_transaction",  # JSONB
    "title", "body", "source",
    "sector", "sub_sector",
    "tags",              # array/jsonb
    "holder_type",
}

@dataclass
class PriceTransaction:
    """
    Standardized Price_Transaction format (Requirement #2)
    This represents a single transaction event.
    """
    transaction_date: Optional[str] = None
    transaction_type: Optional[str] = None # 'buy', 'sell', 'other'
    transaction_price: Optional[float] = None
    transaction_share_amount: Optional[int] = None

@dataclass
class FilingRecord:
    """
    The standardized, canonical filing record.
    All data sources (auto-scraper, manual-entry) must be
    transformed into this structure before processing or uploading.
    """
    # Core Fields
    symbol: str
    timestamp: str  # ISO Format
    transaction_type: str # 'buy', 'sell', 'other'
    holder_name: str
    
    # Holdings
    holding_before: Optional[int] = None
    holding_after: Optional[int] = None
    amount_transaction: Optional[int] = None
    
    # Percentages (Rounded to 5 decimals - Requirement #7)
    share_percentage_before: Optional[float] = None
    share_percentage_after: Optional[float] = None
    share_percentage_transaction: Optional[float] = None
    
    # Price & Value
    price: Optional[float] = None
    transaction_value: Optional[float] = None

    # Standardized JSONB field (Requirement #2)
    price_transaction: List[PriceTransaction] = field(default_factory=list)
    
    # Generated Content (Requirement #3)
    title: Optional[str] = None
    body: Optional[str] = None
    purpose_of_transaction: Optional[str] = None # The original, translated purpose
    
    # Classification (Requirement #5, #6)
    tags: List[str] = field(default_factory=list)
    sector: Optional[str] = None
    sub_sector: Optional[str] = None

    # Source / Meta
    source: Optional[str] = None # PDF URL
    holder_type: Optional[str] = None
    
    # Fields below are for internal processing, not for DB
    
    # Raw data, for reference during processing
    raw_data: Dict[str, Any] = field(default_factory=dict, repr=False)
    
    # Audit flags set by processors.py
    audit_flags: Dict[str, Any] = field(default_factory=dict, repr=False)
    
    # Skip reason for alerts
    skip_reason: Optional[str] = None
    
    
    def to_db_dict(self) -> Dict[str, Any]:
        """
        Converts this dataclass into a dictionary safe for Supabase insertion.
        This is the single source of truth for the DB schema.
        """
        # This set defines the exact columns for the 'idx_filings' table.
        ALLOWED_DB_COLUMNS = {
            "symbol", "timestamp", "transaction_type", "holder_name",
            "holding_before", "holding_after", "amount_transaction",
            "share_percentage_before", "share_percentage_after", "share_percentage_transaction",
            "price", "transaction_value", "price_transaction", "title", "body",
            "purpose_of_transaction", "tags", "sector", "sub_sector", "source", "holder_type",
            # Add 'dedup_hash' if you decide to store it
        }
        
        db_dict = {}
        
        # 1. Handle price_transaction conversion
        if self.price_transaction:
            # Convert List[PriceTransaction] to List[Dict]
            db_dict["price_transaction"] = [tx.__dict__ for tx in self.price_transaction]
        
        # 2. Add all other allowed fields
        for key in ALLOWED_DB_COLUMNS:
            if key == "price_transaction":
                continue # Already handled
            
            val = getattr(self, key, None)
            
            # Only add non-None values to the dict for a clean insert
            if val is not None:
                db_dict[key] = val
                
        # 3. Special handling: ensure sub_sector is a string, not a list
        if isinstance(db_dict.get("sub_sector"), list):
             db_dict["sub_sector"] = db_dict["sub_sector"][0] if db_dict["sub_sector"] else None
        
        return db_dict