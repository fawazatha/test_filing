# src/generate/articles/types.py
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional
from datetime import datetime
import json


def as_jsonable(obj):
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    if isinstance(obj, (list, tuple)):
        return [as_jsonable(x) for x in obj]
    if isinstance(obj, dict):
        return {k: as_jsonable(v) for k, v in obj.items()}
    if hasattr(obj, "__dict__"):
        return as_jsonable(obj.__dict__)
    try:
        return json.loads(json.dumps(obj))
    except Exception:
        return str(obj)


@dataclass
class FilingRow:
    """Minimal subset dari fields `data/filings_data.json` yang kita butuhkan."""
    title: str
    body: str
    source: Optional[str] = None
    timestamp: Optional[str] = None
    symbol: Optional[str] = None
    holder_name: Optional[str] = None
    holder_type: Optional[str] = None
    sector: Optional[str] = None
    sub_sector: Optional[str] = None
    transaction_type: Optional[str] = None
    amount_transaction: Optional[str] = None
    price: Optional[str] = None
    transaction_value: Optional[str] = None
    tags: Optional[str] = None            # serialized JSON list (string) in your current pipeline
    price_transaction: Optional[str] = None  # serialized dict (string)

    raw: Dict[str, Any] = field(default_factory=dict)  # simpan row asli untuk debug

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "FilingRow":
        return FilingRow(
            title=d.get("title") or "",
            body=d.get("body") or "",
            source=d.get("source"),
            timestamp=d.get("timestamp"),
            symbol=d.get("symbol"),
            holder_name=d.get("holder_name"),
            holder_type=d.get("holder_type"),
            sector=d.get("sector"),
            sub_sector=d.get("sub_sector"),
            transaction_type=d.get("transaction_type"),
            amount_transaction=d.get("amount_transaction"),
            price=d.get("price"),
            transaction_value=d.get("transaction_value"),
            tags=d.get("tags"),
            price_transaction=d.get("price_transaction"),
            raw=d,
        )


@dataclass
class ArticleDraft:
    """Draft hasil generasi (sebelum upload)."""
    title: str
    body: str
    tags: List[str] = field(default_factory=list)
    subsector: Optional[str] = None
    sector: Optional[str] = None
    tickers: List[str] = field(default_factory=list)
    sentiment: Optional[str] = None
    source: Optional[str] = None
    timestamp: Optional[str] = None
    symbol: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # pastikan deterministik: sort tags/tickers
        d["tags"] = sorted(set(self.tags))
        d["tickers"] = sorted(set(self.tickers))
        return d


@dataclass
class ArticleBatchResult:
    articles: List[ArticleDraft] = field(default_factory=list)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    debug_records: List[Dict[str, Any]] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps({
            "articles": [a.to_dict() for a in self.articles],
            "errors": self.errors,
        }, ensure_ascii=False, indent=2)
