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

# Month tokens (EN/ID) for adjacency checks (e.g., "30 Okt")
_MONTH_WORD = (
    r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
    r"Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?|"
    r"Jan(?:uari)?|Feb(?:ruari)?|Mar(?:et)?|Apr(?:il)?|Mei|Jun(?:i)?|Jul(?:i)?|"
    r"Agu(?:stus)?|Sep(?:tember)?|Okt(?:ober)?|Nov(?:ember)?|Des(?:ember)?)"
)
_MONTH_RE = re.compile(rf"\b{_MONTH_WORD}\b", re.IGNORECASE)

def _date_spans_in_text(s: str) -> List[tuple[int, int]]:
    """Return spans (start, end) of any date-like substring in s."""
    spans: List[tuple[int, int]] = []
    for m in _DATE_ANY.finditer(s or ""):
        spans.append(m.span())
    # also catch simple 'dd Month' (optionally with year)
    for m in re.finditer(rf"\b\d{{1,2}}\s+{_MONTH_WORD}(?:\s+\d{{2,4}})?\b", s or "", flags=re.IGNORECASE):
        spans.append(m.span())
    return spans

def _token_spans_price_like(s: str) -> List[tuple[str, int, int]]:
    """
    Collect price-looking numeric tokens:
    - 1..6 digits, optional decimal part (no thousands separators)
    """
    out: List[tuple[str, int, int]] = []
    for m in re.finditer(r"[0-9][0-9\.,]*", s or ""):
        t = m.group(0)
        # allow up to 6 integer digits with optional decimal
        if re.fullmatch(r"\d{1,6}(?:[.,]\d{1,2})?", t):
            # exclude thousands-formatted integers like 14.838.000
            if not re.fullmatch(r"\d{1,3}(?:[.,]\d{3})+", t):
                out.append((t, m.start(), m.end()))
    return out

def _span_contains(idx: int, spans: List[tuple[int, int]]) -> bool:
    return any(a <= idx < b for (a, b) in spans)


def _prefer_price_from_line(line: str) -> Optional[str]:
    """
    Pilih token 'harga' dari satu baris dengan heuristik:
    - Hindari token di dalam span tanggal
    - IZINKAN angka bertitik ribuan JIKA ada hint harga ('harga transaksi', 'transaction price', 'harga:')
    - Jika tidak ada hint harga, tetap hindari _RE_BIG_INT (kemungkinan amount)
    """
    if not line:
        return None
    s = line.strip()
    date_spans = _date_spans_in_text(s)
    lwr = s.lower()
    has_price_hint = ("harga transaksi" in lwr) or ("transaction price" in lwr) or ("harga:" in lwr)

    tokens = list(re.finditer(r"[0-9][0-9\.,]*", s))
    if not tokens:
        return None

    def score(tok: str, start: int) -> int:
        sc = 0
        if _span_contains(start, date_spans):
            return -999
        after = s[start:start + 12]
        if _MONTH_RE.search(after):
            return -998
        if has_price_hint:
            sc += 6   # kuatkan jika ada hint harga
        if ("rp" in lwr) or ("idr" in lwr):
            sc += 2
        if ("," in tok or "." in tok):
            sc += 1
        try:
            val = NumberParser.parse_number(tok) or 0
            if val <= 31 and sc < 6:
                sc -= 2
        except Exception:
            pass
        return sc

    best_tok, best_sc = None, -999
    for m in tokens:
        t = m.group(0)
        # aturan lama: hindari _RE_BIG_INT
        # revisi: jika ada hint harga, izinkan _RE_BIG_INT (contoh '2.000')
        if not (_RE_PRICE.fullmatch(t) or (has_price_hint and _RE_BIG_INT.fullmatch(t))):
            continue
        sc = score(t, m.start())
        if sc > best_sc:
            best_sc, best_tok = sc, t
    return best_tok


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
_RE_BIG_INT = re.compile(r"^\d{1,3}(?:[.,]\d{3})+$")   # e.g. 14.838.000
_RE_PRICE   = re.compile(r"^\d{1,6}(?:[.,]\d{1,2})?$") # e.g. 55, 198, 1250, 10150, 75.5
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
            window = self.lines[header_idx + 1: header_idx + 15]
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
        """
        Extract rudimentary 'transfer' rows from narrative lines.
        Use the same price heuristic to avoid catching day-of-month as price.
        """
        rows: List[Dict[str, Any]] = []
        for line in self.lines:
            lo = (line or "").lower()
            if "pengalihan" not in lo:
                continue

            # price (robust, context-aware)
            price_s = _prefer_price_from_line(line)
            price = NumberParser.parse_number(price_s) if price_s else 0

            # amount (prefer thousands-formatted; else last numeric)
            tokens = re.findall(r"\b\d{1,3}(?:[.,]\d{3})+\b|\b\d+\b", line)
            if not tokens:
                continue
            # choose thousands-formatted if exists, else the last numeric
            amt_s = next((t for t in tokens if re.fullmatch(r"\d{1,3}(?:[.,]\d{3})+", t)), tokens[-1])
            amount = NumberParser.parse_number(amt_s)

            # date
            date_norm = parse_id_en_date(line)
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
                cand_price = _prefer_price_from_line(raw)
                if cand_price:
                    row_price_s = cand_price
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
                         self.lines[header_idx + 1: header_idx + 11])
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

        # price (context-aware)
        price_s = None
        for s in window:
            cand = _prefer_price_from_line(s)
            if cand:
                price_s = cand
                break

        # date
        date_s = None
        for s in window:
            if parse_id_en_date(s):
                date_s = s.strip()
                break

        # amount (prefer thousands-separated; else largest numeric)
        amount_s = None
        max_val = -1
        for s in window:
            ss = (s or "").strip()
            if _RE_BIG_INT.fullmatch(ss):
                amount_s = ss
                break
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
        try to harvest price (context-aware), date (first date), and amount
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

            # price (context-aware)
            price_s = _prefer_price_from_line(s)

            # tokens for date/amount
            tokens = re.findall(r"[0-9][0-9\.,]*", s)

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
