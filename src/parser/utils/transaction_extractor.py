from __future__ import annotations
import re
import uuid
import os
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
5) Final fallback: extract price directly from body narrative if table missing.
"""

logger = get_logger(__name__)
logger.debug("[transaction_extractor] imported from: %s", __file__)

# Global constants
MAX_PRICE_IDR = float(os.getenv("PRICE_MAX_IDR", "100000"))  # sensible upper bound for IDR/share

# Date patterns (EN/ID)
_DATE_ANY_STR = f"(?:{PAT_EN_FULL.pattern}|{PAT_ID_FULL.pattern})"
_DATE_ANY = re.compile(_DATE_ANY_STR, re.IGNORECASE)

# Month tokens (EN/ID)
_MONTH_WORD = (
    r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
    r"Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?|"
    r"Jan(?:uari)?|Feb(?:ruari)?|Mar(?:et)?|Apr(?:il)?|Mei|Jun(?:i)?|Jul(?:i)?|"
    r"Agu(?:stus)?|Sep(?:tember)?|Okt(?:ober)?|Nov(?:ember)?|Des(?:ember)?)"
)
_MONTH_RE = re.compile(rf"\b{_MONTH_WORD}\b", re.IGNORECASE)

# Common numeric patterns
_RE_BIG_INT = re.compile(r"^\d{1,3}(?:[.,]\d{3})+$")   # e.g. 14.838.000
_RE_PRICE   = re.compile(r"^\d{1,6}(?:[.,]\d{1,2})?$") # e.g. 55, 198, 1250, 75.5
_RE_ANYNUM  = re.compile(r"^\d+(?:[.,]\d+)*$")

# Small helpers
def _date_spans_in_text(s: str) -> List[tuple[int, int]]:
    spans: List[tuple[int, int]] = []
    for m in _DATE_ANY.finditer(s or ""):
        spans.append(m.span())
    # also catch simple 'dd Month' (optionally with year)
    for m in re.finditer(rf"\b\d{{1,2}}\s+{_MONTH_WORD}(?:\s+\d{{2,4}})?\b", s or "", flags=re.IGNORECASE):
        spans.append(m.span())
    return spans

def _span_contains(idx: int, spans: List[tuple[int, int]]) -> bool:
    return any(a <= idx < b for (a, b) in spans)

def _looks_like_yyyymmdd_int(s: str) -> bool:
    # 8-digit integer likely a compact date, e.g., 20251031
    if not re.fullmatch(r"\d{8}", s or ""):
        return False
    try:
        v = int(s)
        return 19000101 <= v < 21000101
    except Exception:
        return False

def _has_amount_hint(text: str) -> bool:
    l = (text or "").lower()
    return ("jumlah saham" in l) or ("number of shares" in l) or ("shares transacted" in l) or ("saham" in l)

def _has_price_hint(text: str) -> bool:
    l = (text or "").lower()
    return ("harga transaksi" in l) or ("transaction price" in l) or ("harga:" in l) or (" price" in l) or ("rp" in l) or ("idr" in l)

# Price chooser (context-aware)
def _prefer_price_from_line(line: str) -> Optional[str]:
    """
    Robust price extractor.
    - Rejects 'amount' lines and very large values unless explicitly marked as price.
    - Rejects very small values (e.g., '1' from 'Lampiran 1') unless price-hinted.
    - Avoids tokens overlapping with dates/month words.
    """
    if not line:
        return None

    s = line.strip()
    lwr = s.lower()
    date_spans = _date_spans_in_text(s)

    has_price_hint = _has_price_hint(s)
    has_amount_hint = _has_amount_hint(s)
    tokens = list(re.finditer(r"[0-9][0-9\.,]*", s))
    if not tokens:
        return None

    best_tok, best_sc = None, -999
    for m in tokens:
        t = m.group(0)

        # drop thousands-formatted on obvious amount lines
        if has_amount_hint and re.fullmatch(r"\d{1,3}(?:[.,]\d{3})+", t):
            continue

        # numeric value
        try:
            val = NumberParser.parse_number(t) or 0
        except Exception:
            continue

        # reject absurdly large prices unless explicitly price-hinted
        if not has_price_hint and val > MAX_PRICE_IDR:
            continue

        # reject tiny values without a price hint (e.g., '1')
        if not has_price_hint and val < 10:
            continue

        # reject tokens overlapping a date region or adjacent month word
        if _span_contains(m.start(), date_spans):
            continue
        if _MONTH_RE.search(s[m.start():m.start() + 12]):
            continue

        # scoring (kept simple)
        sc = 0
        if has_price_hint: sc += 6
        if ("rp" in lwr) or ("idr" in lwr): sc += 2
        if ("," in t or "." in t): sc += 1
        if val <= 31 and not has_price_hint: sc -= 3
        if val > 100_000: sc -= 4

        if sc > best_sc:
            best_sc, best_tok = sc, t

    return best_tok

# Block-format regex (labeled paragraph style)
_TYPES_ANY = r"(Buy|Sell|Transfer|Pembelian|Penjualan|Pengalihan)"
_BLOCK_RE = re.compile(
    rf"Type of Transaction:\s*({_TYPES_ANY}).*?"
    rf"Transaction Price:\s*([0-9\.,]+).*?"
    rf"Transaction Date:\s*({_DATE_ANY_STR}).*?"
    rf"Number of Shares Transacted:\s*([0-9\.,]+)",
    re.IGNORECASE | re.DOTALL
)

# Extractor
class TransactionExtractor:
    def __init__(self, extractor: TextExtractor, ticker: Optional[str] = None):
        self.ex = extractor
        self.lines = extractor.lines or []
        self.ticker = ticker or "UNKNOWN"

    # Public API
    def extract_transaction_rows(self) -> List[Dict[str, Any]]:
        """
        Try parsers in order of reliability:
        1) Stacked-cell line parser
        2) Block-format parser
        3) Window fallback
        4) Loose single-line parser
        5) Final fallback: narrative price from body
        """
        rows, header_idx = self._parse_transactions_lines()
        if rows:
            logger.debug("Found %d transaction(s) via STACKED-CELL parser.", len(rows))
            return rows

        rows = self._parse_transactions_block()
        if rows:
            logger.debug("Found %d transaction(s) via BLOCK parser.", len(rows))
            return rows

        if header_idx is not None and header_idx >= 0:
            window = self.lines[header_idx + 1: header_idx + 15]
            row = self._parse_transactions_window_fallback(window)
            if row:
                logger.debug("Found 1 transaction via WINDOW fallback.")
                return [row]

        rows = self._parse_transactions_loose_lines()
        if rows:
            logger.debug("Found %d transaction(s) via LOOSE-LINE parser.", len(rows))
            return rows

        # Final fallback: body narrative (price only)
        body_text = getattr(self.ex, "full_text", "\n".join(self.lines))
        price = self._extract_price_from_body(body_text)
        if price:
            logger.debug("Recovered narrative price %.2f from body fallback.", price)
            return [{
                "type": None,
                "price": price,
                "amount": None,
                "value": None,
                "date": None,
                "date_raw": None
            }]

        return []

    # Heuristics
    def _extract_price_from_body(self, body: str) -> Optional[float]:
        """Recover plausible transaction price from narrative text."""
        if not body:
            return None
        tokens = re.findall(r"[0-9][0-9\.,]*", body)
        candidates = []
        for tok in tokens:
            try:
                val = NumberParser.parse_number(tok)
            except Exception:
                continue
            if not val:
                continue
            if 10 <= val <= MAX_PRICE_IDR:
                candidates.append(val)
        if not candidates:
            return None
        return min(candidates)  # smaller value more likely to be price

    def contains_transfer_transaction(self) -> bool:
        for line in self.lines:
            lo = (line or "").lower()
            if "jenis transaksi" in lo or "transaction type" in lo:
                continue
            if "pengalihan" in lo:
                return True
        return False

    def extract_transfer_transactions(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for line in self.lines:
            lo = (line or "").lower()
            if "pengalihan" not in lo:
                continue
            price_s = _prefer_price_from_line(line)
            price = NumberParser.parse_number(price_s) if price_s else 0

            # amount with guards
            tokens = re.findall(r"[0-9][0-9\.,]*", line)
            amt_s = None
            max_val = -1
            for t in tokens[::-1]:
                if _looks_like_yyyymmdd_int(t):
                    continue
                if _RE_BIG_INT.fullmatch(t):
                    amt_s = t
                    break
                if _RE_ANYNUM.fullmatch(t):
                    try:
                        val = NumberParser.parse_number(t) or 0
                    except Exception:
                        val = 0
                    if _has_amount_hint(line) or val >= 1000:
                        if val > max_val:
                            max_val = val
                            amt_s = t
            if not amt_s:
                continue

            amount = NumberParser.parse_number(amt_s)
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

    # Core parsing helpers
    def _row_is_plausible(self, tx_type: str, price: Optional[float], amount: Optional[float], date_s: Optional[str], line_text: Optional[str]) -> bool:
        # Reject YYYYMMDD-shaped amounts
        if amount is not None:
            try:
                amt_int_str = str(int(amount))
                if _looks_like_yyyymmdd_int(amt_int_str):
                    return False
            except Exception:
                pass
        # Price sanity: without a price hint, reject tiny prices
        if (price is not None) and (price < 10) and (not (line_text and _has_price_hint(line_text))):
            return False
        # Upper bound for price
        if (price is not None) and (price > MAX_PRICE_IDR * 2):  # ultra safety
            return False
        return True

    def _push_row(self, kind: str, price_s: str, date_s: Optional[str], amount_s: str) -> Dict[str, Any]:
        k = (kind or "").strip().lower()
        if k in ("buy", "pembelian"):
            tx_type = "buy"
        elif k in ("sell", "penjualan"):
            tx_type = "sell"
        elif k in ("transfer", "pengalihan"):
            tx_type = "transfer"
        else:
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
            "date": date_norm,
        }

    # Parsers
    def _parse_transactions_block(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        full = "\n".join(self.lines)
        for m in _BLOCK_RE.finditer(full):
            try:
                kind, price_s, date_s, amt_s = m.group(1), m.group(2), m.group(3), m.group(4)
                candidate = self._push_row(kind, price_s, date_s, amt_s)
                if self._row_is_plausible(candidate["type"], candidate["price"], candidate["amount"], candidate["date_raw"], m.group(0)):
                    rows.append(candidate)
                else:
                    logger.debug("Rejected implausible block row: %s", candidate)
            except Exception as e:
                logger.warning("Block parser failed: %s", e)
        return rows

    def _parse_transactions_lines(self) -> tuple[List[Dict[str, Any]], Optional[int]]:
        rows: List[Dict[str, Any]] = []
        HEADER_TOKENS = (
            "type of transaction", "transaction price", "transaction date", "number of shares transacted",
            "jenis transaksi", "harga transaksi", "tanggal transaksi", "jumlah saham",
        )
        STOP_TOKENS = (
            "purposes of transaction", "purpose of transaction", "tujuan transaksi",
            "share ownership status", "status kepemilikan saham",
            "number of shares owned after", "percentage of ownership after",
            "respectfully", "hormat",
        )

        def is_header_line(s: str) -> bool:
            return any(tok in (s or "").lower() for tok in HEADER_TOKENS)

        def is_stop(s: str) -> bool:
            return any(tok in (s or "").lower() for tok in STOP_TOKENS)

        header_idx = -1
        for i, line in enumerate(self.lines):
            if is_header_line(line):
                header_idx = i
                break
        if header_idx == -1:
            return [], None

        row_kind = row_price_s = row_date_s = row_amt_s = None
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
            if row_kind and not row_price_s:
                cand_price = _prefer_price_from_line(raw)
                if cand_price:
                    row_price_s = cand_price
                    continue

            # (3) date
            if row_kind and row_price_s and not row_date_s:
                if parse_id_en_date(raw):
                    row_date_s = raw
                    continue

            # (4) amount (prefer thousands-separated; else largest numeric) with guards
            if row_kind and row_price_s and row_date_s and not row_amt_s:
                tokens = re.findall(r"[0-9][0-9\.,]*", raw)
                cand = None
                max_val = -1
                for t in tokens[::-1]:
                    if _looks_like_yyyymmdd_int(t):
                        continue
                    if _RE_BIG_INT.fullmatch(t):
                        cand = t
                        break
                    if _RE_ANYNUM.fullmatch(t):
                        try:
                            val = NumberParser.parse_number(t) or 0
                        except Exception:
                            val = 0
                        if _has_amount_hint(raw) or val >= 1000:
                            if val > max_val:
                                max_val = val
                                cand = t
                if cand:
                    row_amt_s = cand

            # finalize
            if row_kind and row_price_s and row_date_s and row_amt_s:
                try:
                    candidate = self._push_row(row_kind, row_price_s, row_date_s, row_amt_s)
                    if self._row_is_plausible(candidate["type"], candidate["price"], candidate["amount"], candidate["date_raw"], raw):
                        rows.append(candidate)
                    else:
                        logger.debug("Rejected implausible stacked-cell row: %s", candidate)
                except Exception as e:
                    logger.warning("Line parser failed: %s", e)
                row_kind = row_price_s = row_date_s = row_amt_s = None

        if not rows:
            logger.debug("Stacked-cell parser found 0 rows. Window after header: %s",
                         self.lines[header_idx + 1: header_idx + 11])
        return rows, header_idx

    def _parse_transactions_window_fallback(self, window: List[str]) -> Optional[Dict[str, Any]]:
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

        # price
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

        # amount (prefer thousands-separated; else largest numeric) with guards
        amount_s = None
        max_val = -1
        for s in window:
            ss = (s or "").strip()
            tokens = re.findall(r"[0-9][0-9\.,]*", ss)
            for t in tokens[::-1]:
                if _looks_like_yyyymmdd_int(t):
                    continue
                if _RE_BIG_INT.fullmatch(t):
                    amount_s = t
                    break
                if _RE_ANYNUM.fullmatch(t):
                    try:
                        val = NumberParser.parse_number(t) or 0
                    except Exception:
                        val = 0
                    if _has_amount_hint(ss) or val >= 1000:
                        if val > max_val:
                            max_val = val
                            amount_s = t
            if amount_s:
                break

        if kind and price_s and date_s and amount_s:
            try:
                candidate = self._push_row(kind, price_s, date_s, amount_s)
                if self._row_is_plausible(candidate["type"], candidate["price"], candidate["amount"], candidate["date_raw"], " ".join(window)):
                    logger.debug("Window fallback produced row: %s", candidate)
                    return candidate
                logger.debug("Rejected implausible window row: %s", candidate)
            except Exception as e:
                logger.warning("Window fallback failed: %s", e)
        else:
            logger.debug("Window fallback incomplete: kind=%s price=%s date=%s amount=%s",
                         kind, price_s, date_s, amount_s)
        return None

    def _parse_transactions_loose_lines(self) -> List[Dict[str, Any]]:
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

            # price
            price_s = _prefer_price_from_line(s)

            # date
            dm = _DATE_ANY.search(s)
            date_s = dm.group(0) if dm else None

            # amount with guards
            amount_s = None
            tokens = re.findall(r"[0-9][0-9\.,]*", s)
            max_val = -1
            for t in tokens[::-1]:
                if _looks_like_yyyymmdd_int(t):
                    continue
                if _RE_BIG_INT.fullmatch(t):
                    amount_s = t
                    break
                if _RE_ANYNUM.fullmatch(t):
                    try:
                        val = NumberParser.parse_number(t) or 0
                    except Exception:
                        val = 0
                    if _has_amount_hint(s) or val >= 1000:
                        if val > max_val:
                            max_val = val
                            amount_s = t

            if kind and price_s and amount_s:
                try:
                    candidate = self._push_row(kind, price_s, date_s, amount_s)
                    if self._row_is_plausible(candidate["type"], candidate["price"], candidate["amount"], candidate["date_raw"], s):
                        rows.append(candidate)
                    else:
                        logger.debug("Rejected implausible loose-line row: %s", candidate)
                except Exception as e:
                    logger.warning("Loose-line row failed: %s", e)
        return rows
