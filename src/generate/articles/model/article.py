from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List, Optional
from datetime import datetime

@dataclass
class Article:
    """
    Represents a complete article structure.
    
    This class encapsulates all the data needed for a complete article,
    including metadata, content, and financial information.
    """
    # Core metadata
    title: str = ""
    body: str = ""
    source: str = ""
    timestamp: str = ""
    uid: Optional[str] = None
    
    # Classification data
    sector: str = ""
    sub_sector: str = ""
    tags: List[str] = field(default_factory=lambda: ["insider-trading"])
    tickers: List[str] = field(default_factory=list)
    
    # Transaction data
    transaction_type: str = ""
    holder_type: str = ""
    holder_name: str = ""
    
    # Financial data
    holding_before: int = 0
    holding_after: int = 0
    share_percentage_before: float = 0.0
    share_percentage_after: float = 0.0
    share_percentage_transaction: float = 0.0
    amount_transaction: int = 0
    
    # Price data
    price: int = 0
    transaction_value: int = 0
    price_transaction: Dict[str, List[int]] = field(
        default_factory=lambda: {"prices": [], "amount_transacted": []}
    )
    
    # Internal fields (not included in final output)
    _purpose: str = field(default="", repr=False)

    @classmethod
    def create_initial_structure(
        cls,
        pdf_url: str,
        sector: str,
        sub_sector: str,
        holder_type: str,
        uid: Optional[str] = None
    ) -> 'Article':
        """
        Create an initial article structure with basic metadata.

        Args:
            pdf_url (str): URL of the source PDF
            sector (str): Company sector
            sub_sector (str): Company sub-sector
            holder_type (str): Type of holder
            uid (Optional[str]): Unique identifier

        Returns:
            Article: Initial article structure
        """
        return cls(
            source=pdf_url,
            sector=sector,
            sub_sector=sub_sector,
            holder_type=holder_type,
            uid=uid,
            timestamp="",  # Will be populated later
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert article to dictionary format, excluding internal fields.

        Returns:
            Dict[str, Any]: Article data as dictionary
        """
        # Get all fields as dict
        result = asdict(self)
        
        # Remove internal fields (those starting with _)
        result = {k: v for k, v in result.items() if not k.startswith('_')}
        
        return result

    def add_ticker(self, ticker: str) -> None:
        """
        Add a ticker to the article, ensuring proper format.

        Args:
            ticker (str): Ticker symbol to add
        """
        if ticker:
            formatted_ticker = ticker.upper()
            if not formatted_ticker.endswith('.JK'):
                formatted_ticker += '.JK'
            
            if formatted_ticker not in self.tickers:
                self.tickers.append(formatted_ticker)

    def set_timestamp_from_string(self, date_time_str: str) -> None:
        """
        Set timestamp from a date string, handling various formats.

        Args:
            date_time_str (str): Date time string to parse
        """
        if not date_time_str:
            return
            
        try:
            # Handle dd-mm-YYYY HH:MM format (add :00 if needed)
            if re.match(r"\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}$", date_time_str):
                date_time_str += ":00"
            
            parsed = datetime.strptime(date_time_str, "%d-%m-%Y %H:%M:%S")
            self.timestamp = parsed.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            # Keep original string if parsing fails
            self.timestamp = date_time_str

    def determine_transaction_type(self) -> str:
        """
        Determine transaction type based on holding changes.

        Returns:
            str: Transaction type ('buy', 'sell', or empty string)
        """
        if self.holding_before and self.holding_after:
            return "buy" if self.holding_after > self.holding_before else "sell"
        elif self.share_percentage_after > self.share_percentage_before:
            return "buy"
        elif self.share_percentage_after < self.share_percentage_before:
            return "sell"
        return ""

    def calculate_amount_transaction(self) -> int:
        """
        Calculate transaction amount based on holding changes.

        Returns:
            int: Transaction amount
        """
        if self.holding_before and self.holding_after:
            return abs(self.holding_after - self.holding_before)
        return 0

    def is_valid(self) -> bool:
        """
        Check if the article has minimum required data.

        Returns:
            bool: True if article has minimum required data
        """
        return bool(
            self.title and 
            self.body and 
            (self.holder_name or self.tickers)
        )