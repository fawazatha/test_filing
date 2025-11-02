# src/core/types.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

# Kolom yang diizinkan untuk upload (boleh dipakai oleh uploader)
FILINGS_ALLOWED_COLUMNS = {
    "symbol", "timestamp", "transaction_type", "holder_name",
    "holding_before", "holding_after", "amount_transaction",
    "share_percentage_before", "share_percentage_after", "share_percentage_transaction",
    "price", "transaction_value",
    "price_transaction",              # JSONB (akan di-collapse saat to_db_dict)
    "title", "body", "source",
    "sector", "sub_sector",
    "tags",                           # array/jsonb
    "holder_type",
    "purpose_of_transaction",         # keep for completeness (walau bukan kolom wajib)
}


@dataclass
class PriceTransaction:
    """
    Standardized Price_Transaction (internal representation).
    """
    transaction_date: Optional[str] = None        # "YYYY-MM-DD" (prefer) atau ISO full
    transaction_type: Optional[str] = None        # 'buy' | 'sell' | 'other' | ...
    transaction_price: Optional[float] = None
    transaction_share_amount: Optional[int] = None


@dataclass
class FilingRecord:
    """
    Canonical filing record (internal). Semua sumber harus diubah ke format ini
    sebelum diproses/diupload. Serialisasi ke DB dilakukan oleh to_db_dict().
    """
    # Core
    symbol: str
    timestamp: str                  # ISO string
    transaction_type: str           # 'buy' | 'sell' | 'other' | ...
    holder_name: str

    # Holdings
    holding_before: Optional[int] = None
    holding_after: Optional[int] = None
    amount_transaction: Optional[int] = None

    # Percentages
    share_percentage_before: Optional[float] = None
    share_percentage_after: Optional[float] = None
    share_percentage_transaction: Optional[float] = None

    # Price & Value (level atas)
    price: Optional[float] = None
    transaction_value: Optional[float] = None

    # Standardized JSONB (internal list; akan di-collapse saat to_db_dict)
    price_transaction: List[PriceTransaction] = field(default_factory=list)

    # Generated content
    title: Optional[str] = None
    body: Optional[str] = None
    purpose_of_transaction: Optional[str] = None

    # Classification
    tags: List[str] = field(default_factory=list)
    sector: Optional[str] = None
    sub_sector: Optional[str] = None

    # Source / Meta
    source: Optional[str] = None
    holder_type: Optional[str] = None

    # --- Non-DB fields (internal use only) ---
    raw_data: Dict[str, Any] = field(default_factory=dict, repr=False)
    audit_flags: Dict[str, Any] = field(default_factory=dict, repr=False)
    skip_reason: Optional[str] = None

    # Helpers (private)
    @staticmethod
    def _ensure_date_yyyy_mm_dd(s: Optional[str]) -> Optional[str]:
        """
        Terima "YYYY-MM-DD" atau ISO "YYYY-MM-DDTHH:MM:SS[Z]" → kembalikan "YYYY-MM-DD".
        """
        if not s:
            return None
        return str(s)[:10]

    def _normalize_pt_list_to_objects(self) -> List[PriceTransaction]:
        """
        Backward compatibility:
        - Jika self.price_transaction sudah List[PriceTransaction], return apa adanya.
        - Jika ternyata List[dict] (legacy), konversi ke PriceTransaction.
        """
        if not self.price_transaction:
            return []

        # Sudah object?
        if isinstance(self.price_transaction[0], PriceTransaction):
            return self.price_transaction

        # Legacy: list of dicts
        out: List[PriceTransaction] = []
        for item in self.price_transaction:
            if isinstance(item, PriceTransaction):
                out.append(item)
                continue
            if not isinstance(item, dict):
                # skip yang tidak valid
                continue
            out.append(
                PriceTransaction(
                    transaction_date=item.get("transaction_date") or item.get("date"),
                    transaction_type=item.get("transaction_type") or item.get("type"),
                    transaction_price=item.get("transaction_price") or item.get("price"),
                    transaction_share_amount=(
                        item.get("transaction_share_amount")
                        or item.get("amount")
                        or item.get("amount_transacted")
                    ),
                )
            )
        return out

    def _collapse_price_transactions_for_db(self) -> List[Dict[str, Any]]:
        """
        Konversi List[PriceTransaction] -> FORMAT DB:
        [
          {
            "date": [...],
            "type": [...],
            "price": [...],
            "amount_transacted": [...]
          }
        ]
        - Fallback tanggal ambil dari self.timestamp kalau tx.date kosong.
        - Pertahankan urutan transaksi seperti internal list.
        """
        tx_list = self._normalize_pt_list_to_objects()

        dates: List[Optional[str]] = []
        types: List[Optional[str]] = []
        prices: List[Optional[float]] = []
        amounts: List[Optional[int]] = []

        fallback_day = self._ensure_date_yyyy_mm_dd(getattr(self, "timestamp", None))

        for tx in (tx_list or []):
            d = self._ensure_date_yyyy_mm_dd(getattr(tx, "transaction_date", None)) or fallback_day
            t = getattr(tx, "transaction_type", None) or self.transaction_type or "other"
            p = getattr(tx, "transaction_price", None)
            a = getattr(tx, "transaction_share_amount", None)

            dates.append(d)
            types.append(t)
            prices.append(p)
            amounts.append(a)

        return [{
            "date": dates,
            "type": types,
            "price": prices,
            "amount_transacted": amounts,
        }]

    # Public: serialize for DB
    def to_db_dict(self) -> Dict[str, Any]:
        """
        Ubah dataclass → dict aman untuk insert Supabase (idx_filings).
        - price_transaction di-collapse ke array-of-lists (sesuai requirement).
        - sector/sub_sector diisi 'unknown' bila None (untuk NOT NULL di DB).
        - sub_sector dipastikan string (bukan list).
        """
        # Kolom DB yang digunakan
        ALLOWED_DB_COLUMNS = {
            "symbol", "timestamp", "transaction_type", "holder_name",
            "holding_before", "holding_after", "amount_transaction",
            "share_percentage_before", "share_percentage_after", "share_percentage_transaction",
            "price", "transaction_value", "price_transaction", "title", "body",
            "purpose_of_transaction", "tags", "sector", "sub_sector", "source", "holder_type",
        }

        db_dict: Dict[str, Any] = {}

        # 1) price_transaction → collapse
        db_dict["price_transaction"] = self._collapse_price_transactions_for_db()

        # 2) kolom lain
        for key in ALLOWED_DB_COLUMNS:
            if key == "price_transaction":
                continue
            val = getattr(self, key, None)
            if val is not None:
                db_dict[key] = val

        # 3) sub_sector harus string tunggal
        if isinstance(db_dict.get("sub_sector"), list):
            db_dict["sub_sector"] = db_dict["sub_sector"][0] if db_dict["sub_sector"] else None

        # 4) default sector/sub_sector (NOT NULL safety)
        if not db_dict.get("sector"):
            db_dict["sector"] = "unknown"
        if not db_dict.get("sub_sector"):
            db_dict["sub_sector"] = "unknown"

        return db_dict
