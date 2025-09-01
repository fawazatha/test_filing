from typing import TypedDict, Literal, List, Optional, Dict, Any

Direction = Literal["buy", "sell", "transfer", "no_change", "unknown"]

class ParsedFiling(TypedDict, total=False):
    source: Dict[str, Any]
    header: Dict[str, Any]
    holder: Dict[str, Any]
    positions: Dict[str, Any]
    legs: List[Dict[str, Any]]
    classification: Dict[str, Any]
    validation: Dict[str, Any]