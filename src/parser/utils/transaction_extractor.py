from __future__ import annotations
import re
import uuid
from typing import Dict, List, Any, Optional

from src.common.log import get_logger
from src.common.numbers import NumberParser
from src.common.datetime import PAT_EN_FULL, PAT_ID_FULL, parse_id_en_date
from .text_extractor import TextExtractor

"""
Robust transaction extractor for IDX-format PDFs.

It supports:
1) Stacked-cell tables with headers possibly split across lines.
2) "Block" format (Type/Price/Date/Amount in labeled paragraphs).
3) Last-resort fallback that assembles a quartet (type, price, date, amount)
   from a small window after the header, without assuming strict order.
4) Loose single-line rows when no explicit header is found.
"""

logger = get_logger(__name__)
logger.debug("[transaction_extractor] imported from: %s", __file__)

# Date patterns (EN/ID)
_DATE_ANY_STR = f"(?:{PAT_EN_FULL.pattern}|{PAT_ID_FULL.pattern})"
_DATE_ANY = re.compile(_DATE_ANY_STR, re.IGNORECASE)

# Transaction keywords
_TYPES_ANY = r"(Buy|Sell|Transfer|Pembelian|Penjualan|Pengalihan)"

# Block-format regex (labeled paragraph style)
_BLOCK_RE = re.compile(
    rf"Type of Transaction:\s*({_TYPES_ANY}).*?"
    rf"Transaction Price:\s*([0-9\.,]+).*?"
    rf"Transaction Date:\s*({_DATE_ANY_STR}).*?"
    rf"Number of Shares Transacted:\s*([0-9\.,]+)",
    re.IGNORECASE | re.DOTALL
)

# Common numeric helpers
_RE_BIG_INT = re.compile(r"^\d{1,3}(?:[.,]\d{3})+$")     # e.g. 14.838.000
_RE_PRICE   = re.compile(r"^\d{1,3}(?:[.,]\d{1,2})?$")   # e.g. 55 or 55.5
_RE_ANYNUM  = re.compile(r"^\d+(?:[.,]\d+)*$")


class TransactionExtractor:
    def __init__(self, extractor: TextExtractor, ticker: Optional[str] = None):
        self.ex = extractor
        self.lines = extractor.lines or []
        self.ticker = ticker or "UNKNOWN"

    #-- Public API--
    def extract_transaction_rows(self) -> List[Dict[str, Any]]:
        """
        Try parsers in order of reliability:
        1) Stacked-cell line parser (tolerant to split headers).
        2) Block-format parser.
        3) If still nothing and a header exists, use a window fallback.
        4) As a last attempt, try loose single-line parsing without header.
        """
        # 1) stacked-cell
        rows, header_idx = self._parse_transactions_lines()
        if rows:
            logger.debug("Found %d transaction(s) via STACKED-CELL parser.", len(rows))
            return rows

        # 2) block
        rows = self._parse_transactions_block()
        if rows:
            logger.debug("Found %d transaction(s) via BLOCK parser.", len(rows))
            return rows

        # 3) window fallback (only meaningful if we saw a header location)
        if header_idx is not None and header_idx >= 0:
            window = self.lines[header_idx + 1 : header_idx + 15]
            row = self._parse_transactions_window_fallback(window)
            if row:
                logger.debug("Found 1 transaction via WINDOW fallback.")
                return [row]

        # 4) loose single-line (no header)
        rows = self._parse_transactions_loose_lines()
        if rows:
            logger.debug("Found %d transaction(s) via LOOSE-LINE parser.", len(rows))
        return rows

    def contains_transfer_transaction(self) -> bool:
        """Heuristic: does text mention 'pengalihan' outside headers?"""
        for line in self.lines:
            lo = (line or "").lower()
            if "jenis transaksi" in lo or "transaction type" in lo:
                continue
            if "pengalihan" in lo:
                return True
        return False

    def extract_transfer_transactions(self) -> List[Dict[str, Any]]:
        """Extract rudimentary 'transfer' rows from narrative lines."""
        rows: List[Dict[str, Any]] = []
        for line in self.lines:
            lo = (line or "").lower()
            if "pengalihan" not in lo:
                continue

            date_norm = parse_id_en_date(line)
            m_price = re.search(r"\b\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{1,2})?\b", line)
            price = NumberParser.parse_number(m_price.group(0)) if m_price else 0

            amount_tokens = re.findall(r"\b\d{1,3}(?:[.,]\d{3})+\b", line) or re.findall(r"\b\d+\b", line)
            if not amount_tokens:
                continue

            amt_s = amount_tokens[-1]
            amount = NumberParser.parse_number(amt_s)
            yyyymmdd = date_norm or ""
            uid_str = f"{self.ticker}-{yyyymmdd}-{amount}-{price}"
            transfer_uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, uid_str))

            rows.append({
                "type": "transfer",
                "price": price,
                "amount": amount,
                "value": (price or 0) * (amount or 0),
                "transfer_uid": transfer_uid,
                "date": yyyymmdd,
                "date_raw": yyyymmdd,
            })
        return rows

    # Parsers
    def _push_row(self, kind: str, price_s: str, date_s: Optional[str], amount_s: str) -> Dict[str, Any]:
        k = (kind or "").strip().lower()
        if k in ("buy", "pembelian"):
            tx_type = "buy"
        elif k in ("sell", "penjualan"):
            tx_type = "sell"
        elif k in ("transfer", "pengalihan"):
            tx_type = "transfer"
        else:
            # Unknown token -> ignore row
            raise ValueError(f"Unknown transaction type: {kind}")

        price = NumberParser.parse_number(price_s)
        amount = NumberParser.parse_number(amount_s)
        date_norm = parse_id_en_date((date_s or "").strip())

        return {
            "type": tx_type,
            "price": price,
            "amount": amount,
            "value": (price or 0) * (amount or 0),
            "date_raw": (date_s or "").strip(),
            "date": date_norm,  # YYYYMMDD or None
        }

    def _parse_transactions_block(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        full = "\n".join(self.lines)
        for m in _BLOCK_RE.finditer(full):
            try:
                kind, price_s, date_s, amt_s = m.group(1), m.group(2), m.group(3), m.group(4)
                rows.append(self._push_row(kind, price_s, date_s, amt_s))
            except Exception as e:
                logger.warning("Block parser failed: %s", e)
        return rows

    def _parse_transactions_lines(self) -> tuple[List[Dict[str, Any]], Optional[int]]:
        """
        Stacked-cell line parser.
        Returns (rows, header_idx). If header not found, header_idx = None.
        """
        rows: List[Dict[str, Any]] = []

        HEADER_TOKENS = (
            # EN
            "type of transaction", "transaction price", "transaction date", "number of shares transacted",
            # ID
            "jenis transaksi", "harga transaksi", "tanggal transaksi", "jumlah saham",
        )
        STOP_TOKENS = (
            "purposes of transaction", "purpose of transaction", "tujuan transaksi",
            "share ownership status", "status kepemilikan saham",
            "number of shares owned after", "percentage of ownership after",
            "respectfully", "hormat",
        )

        def is_header_line(s: str) -> bool:
            ls = (s or "").lower()
            return any(tok in ls for tok in HEADER_TOKENS)

        def is_stop(s: str) -> bool:
            ls = (s or "").lower()
            return any(tok in ls for tok in STOP_TOKENS)

        # find first header-ish line
        header_idx = -1
        for i, line in enumerate(self.lines):
            if is_header_line(line):
                header_idx = i
                break
        if header_idx == -1:
            return [], None

        # state machine: kind -> price -> date -> amount
        row_kind: Optional[str] = None
        row_price_s: Optional[str] = None
        row_date_s: Optional[str] = None
        row_amt_s: Optional[str] = None

        j = header_idx + 1
        while j < len(self.lines):
            raw = (self.lines[j] or "").strip()
            j += 1

            if not raw:
                continue
            if is_stop(raw):
                break
            if is_header_line(raw):
                continue

            lo = raw.lower()

            # (1) kind
            if row_kind is None:
                if any(k in lo for k in ("buy", "sell", "pembelian", "penjualan", "pengalihan", "transfer")):
                    row_kind = "buy" if ("buy" in lo or "pembelian" in lo) else \
                               "sell" if ("sell" in lo or "penjualan" in lo) else "transfer"
                    continue

            # (2) price
            if row_kind is not None and row_price_s is None:
                if _RE_PRICE.match(raw) and not _RE_BIG_INT.match(raw):
                    row_price_s = raw
                    continue

            # (3) date
            if row_kind is not None and row_price_s is not None and row_date_s is None:
                if parse_id_en_date(raw):
                    row_date_s = raw
                    continue

            # (4) amount
            if row_kind is not None and row_price_s is not None and row_date_s is not None and row_amt_s is None:
                if _RE_BIG_INT.match(raw) or _RE_ANYNUM.match(raw):
                    row_amt_s = raw

            # finalize
            if row_kind and row_price_s and row_date_s and row_amt_s:
                try:
                    rows.append(self._push_row(row_kind, row_price_s, row_date_s, row_amt_s))
                except Exception as e:
                    logger.warning("Line parser failed to build row: %s | data=(%s,%s,%s,%s)",
                                   e, row_kind, row_price_s, row_date_s, row_amt_s)
                row_kind = row_price_s = row_date_s = row_amt_s = None

        # diagnostic if nothing found
        if not rows:
            logger.debug("Stacked-cell parser found 0 rows. Window after header: %s",
                         self.lines[header_idx + 1 : header_idx + 11])
        return rows, header_idx

    def _parse_transactions_window_fallback(self, window: List[str]) -> Optional[Dict[str, Any]]:
        """
        Last-resort: pick best candidates for kind/price/date/amount from a small window.
        """
        if not window:
            return None

        # kind
        kind = None
        for s in window:
            ls = (s or "").lower()
            if "buy" in ls or "pembelian" in ls:
                kind = "buy"; break
            if "sell" in ls or "penjualan" in ls:
                kind = "sell"; break
            if "transfer" in ls or "pengalihan" in ls:
                kind = "transfer"; break

        # price (small number, not thousands-separated)
        price_s = None
        for s in window:
            ss = (s or "").strip()
            if _RE_PRICE.fullmatch(ss) and not _RE_BIG_INT.fullmatch(ss):
                price_s = ss; break

        # date
        date_s = None
        for s in window:
            if parse_id_en_date(s):
                date_s = s.strip(); break

        # amount (prefer thousands-separated; else largest numeric)
        amount_s = None
        max_val = -1
        for s in window:
            ss = (s or "").strip()
            if _RE_BIG_INT.fullmatch(ss):
                amount_s = ss; break
            if _RE_ANYNUM.fullmatch(ss):
                try:
                    val = NumberParser.parse_number(ss) or 0
                except Exception:
                    val = 0
                if val > max_val:
                    max_val = val
                    amount_s = ss

        if kind and price_s and date_s and amount_s:
            try:
                row = self._push_row(kind, price_s, date_s, amount_s)
                logger.debug("Window fallback produced row: %s", row)
                return row
            except Exception as e:
                logger.warning("Window fallback failed: %s", e)
        else:
            logger.debug("Window fallback incomplete: kind=%s price=%s date=%s amount=%s",
                         kind, price_s, date_s, amount_s)
        return None

    def _parse_transactions_loose_lines(self) -> List[Dict[str, Any]]:
        """
        No header found. Scan all lines; if a line mentions buy/sell/transfer,
        try to harvest price (first small number), date (first date), and amount
        (largest number or thousands-separated).
        """
        rows: List[Dict[str, Any]] = []
        for row in self.lines:
            s = (row or "").strip()
            if not s:
                continue
            lo = s.lower()
            if not any(k in lo for k in ("buy", "sell", "pembelian", "penjualan", "transfer", "pengalihan")):
                continue

            # kind
            kind = "transfer"
            if "buy" in lo or "pembelian" in lo:
                kind = "buy"
            elif "sell" in lo or "penjualan" in lo:
                kind = "sell"

            # price (first small number not thousand-formatted)
            price_s = None
            tokens = re.findall(r"[0-9][0-9\.,]*", s)
            for t in tokens:
                if _RE_PRICE.fullmatch(t) and not _RE_BIG_INT.fullmatch(t):
                    price_s = t
                    break

            # date
            dm = _DATE_ANY.search(s)
            date_s = dm.group(0) if dm else None

            # amount (prefer thousands-separated; else largest numeric)
            amount_s = None
            max_val = -1
            for t in tokens[::-1]:
                if _RE_BIG_INT.fullmatch(t):
                    amount_s = t
                    break
                if _RE_ANYNUM.fullmatch(t):
                    try:
                        val = NumberParser.parse_number(t) or 0
                    except Exception:
                        val = 0
                    if val > max_val:
                        max_val = val
                        amount_s = t

            if kind and price_s and amount_s:
                try:
                    rows.append(self._push_row(kind, price_s, date_s, amount_s))
                except Exception as e:
                    logger.warning("Loose-line row failed: %s | data=(%s,%s,%s,%s)",
                                   e, kind, price_s, date_s, amount_s)
        return rows
