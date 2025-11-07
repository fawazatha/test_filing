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

Parsers (urutan reliabilitas):
1) Stacked-cell table (header bisa terpecah).
2) Block-format (labeled paragraph).
3) Window fallback (quartet dari jendela kecil setelah header).
4) Loose single-line (tanpa header) — kini wajib konteks harga.
5) Final fallback: estimate harga dari narasi — hanya jika benar-benar
   tidak ada baris transaksi yang terdeteksi.
"""

logger = get_logger(__name__)
logger.debug("[transaction_extractor] imported from: %s", __file__)

# Global constants
# Batasi harga wajar (IDR per saham); bisa override via ENV
MAX_PRICE_IDR = float(os.getenv("PRICE_MAX_IDR", "20000"))

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
_RE_BIG_INT = re.compile(r"^\d{1,3}(?:[.,]\d{3})+$")     # e.g. 14.838.000
_RE_PRICE   = re.compile(r"^\d{2,5}(?:[.,]\d{1,2})?$")   # 2–5 digit, opsional 2 desimal
_RE_ANYNUM  = re.compile(r"^\d+(?:[.,]\d+)*$")

# Context keywords
PRICE_HINTS = (
    "harga transaksi", "transaction price", "harga:", "price",
    "harga per saham", "unit price"
)
AMOUNT_HINTS = (
    "jumlah saham", "number of shares", "shares transacted",
    "jumlah saham yang ditransaksikan", "shares"
)
PERCENT_HINTS = (
    "persentase", "percentage", "%"
)

def _has_any(s: str, toks: tuple[str, ...] | List[str]) -> bool:
    l = (s or "").lower()
    return any(t in l for t in toks)

# Helpers
def _date_spans_in_text(s: str) -> List[tuple[int, int]]:
    spans: List[tuple[int, int]] = []
    for m in _DATE_ANY.finditer(s or ""):
        spans.append(m.span())
    for m in re.finditer(rf"\b\d{{1,2}}\s+{_MONTH_WORD}(?:\s+\d{{2,4}})?\b", s or "", flags=re.IGNORECASE):
        spans.append(m.span())
    return spans

def _span_contains(idx: int, spans: List[tuple[int, int]]) -> bool:
    return any(a <= idx < b for (a, b) in spans)

def _looks_like_compact_date(tok: str) -> bool:
    """
    Deteksi token 'YYYYMMDD' (atau 'YYYYMMD', dll) yang sering tersedot sebagai amount.
    """
    if not re.fullmatch(r"\d{8}", tok):
        return False
    y = int(tok[0:4])
    m = int(tok[4:6])
    d = int(tok[6:8])
    if not (1900 <= y <= 2100):
        return False
    if not (1 <= m <= 12):
        return False
    if not (1 <= d <= 31):
        return False
    return True

def _ok_price_token(tok: str) -> bool:
    # kandidat harga yang wajar: 2–5 digit dgn opsional 2 desimal, TANPA format ribuan
    if _RE_BIG_INT.fullmatch(tok):
        return False
    return bool(_RE_PRICE.fullmatch(tok))

def _prefer_price_from_line(line: str) -> Optional[str]:
    """
    Pilih harga hanya dari baris ber-konteks harga.
    Tolak baris persentase & jumlah saham; hindari angka tanggal.
    """
    if not line:
        return None

    s = line.strip()
    lwr = s.lower()
    date_spans = _date_spans_in_text(s)

    has_price_hint   = _has_any(lwr, PRICE_HINTS)
    has_amount_hint  = _has_any(lwr, AMOUNT_HINTS)
    has_percent_hint = _has_any(lwr, PERCENT_HINTS)

    # Baris yang jelas bukan harga -> out
    if has_amount_hint or has_percent_hint:
        return None

    tokens = list(re.finditer(r"[0-9][0-9\.,]*", s))
    if not tokens:
        return None

    best_tok, best_sc = None, -10_000
    for m in tokens:
        t = m.group(0)

        # tolak angka yang mirip tanggal kompak
        if _looks_like_compact_date(t):
            continue

        if not _ok_price_token(t):
            continue

        # tolak angka yang bagian dari tanggal / dekat nama bulan
        if _span_contains(m.start(), date_spans):
            continue
        if _MONTH_RE.search(s[m.start():m.start()+12]):
            continue

        try:
            val = NumberParser.parse_number(t) or 0
        except Exception:
            continue

        # rentang harga wajar
        if not (20 <= val <= MAX_PRICE_IDR):
            continue

        sc = 0
        if has_price_hint:
            sc += 100
        if ("rp" in lwr) or ("idr" in lwr):
            sc += 5
        if 100 <= val <= 9999:
            sc += 3

        if sc > best_sc:
            best_sc, best_tok = sc, t

    return best_tok

# Block-format
_TYPES_ANY = r"(Buy|Sell|Transfer|Pembelian|Penjualan|Pengalihan)"
_BLOCK_RE = re.compile(
    rf"Type of Transaction:\s*({_TYPES_ANY}).*?"
    rf"Transaction Price:\s*([0-9\.,]+).*?"
    rf"Transaction Date:\s*({_DATE_ANY_STR}).*?"
    rf"Number of Shares Transacted:\s*([0-9\.,]+)",
    re.IGNORECASE | re.DOTALL
)

# Main extractor
class TransactionExtractor:
    def __init__(self, extractor: TextExtractor, ticker: Optional[str] = None):
        self.ex = extractor
        self.lines = extractor.lines or []
        self.ticker = ticker or "UNKNOWN"

    def extract_transaction_rows(self) -> List[Dict[str, Any]]:
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

        # Final fallback: hanya jika benar-benar tidak ada baris transaksi
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

    def _extract_price_from_body(self, body: str) -> Optional[float]:
        if not body:
            return None
        tokens = re.findall(r"[0-9][0-9\.,]*", body)
        candidates = []
        for tok in tokens:
            if _looks_like_compact_date(tok):
                continue
            try:
                val = NumberParser.parse_number(tok)
            except Exception:
                continue
            if 20 <= (val or 0) <= MAX_PRICE_IDR:
                candidates.append(val)
        if not candidates:
            return None
        # ambil yang paling kecil (sering lebih dekat ke harga/unit)
        return min(candidates)

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
            tokens = re.findall(r"\b\d{1,3}(?:[.,]\d{3})+\b|\b\d+\b", line)
            if not tokens:
                continue
            # pilih thousands-formatted; hindari tanggal kompak
            amt_s = next(
                (t for t in tokens if re.fullmatch(r"\d{1,3}(?:[.,]\d{3})+", t)),
                None
            )
            if not amt_s:
                # fallback: pilih angka terbesar yang bukan tanggal kompak
                max_val, amt_s = -1, None
                for t in tokens:
                    if _looks_like_compact_date(t):
                        continue
                    if _RE_ANYNUM.fullmatch(t):
                        try:
                            v = NumberParser.parse_number(t) or 0
                        except Exception:
                            v = 0
                        if v > max_val:
                            max_val, amt_s = v, t
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

    # ---------------- core parsers ----------------
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

            # (2) price — cari baris berlabel harga dalam lookahead pendek;
            # atau angka kecil wajar yang bukan jumlah & bukan tanggal.
            if row_kind is not None and row_price_s is None:
                lookahead = self.lines[j-1 : min(j-1+6, len(self.lines))]
                chosen = None
                for la in lookahead:
                    la_s = (la or "").strip()
                    la_l = la_s.lower()

                    if _has_any(la_l, AMOUNT_HINTS) or _has_any(la_l, PERCENT_HINTS):
                        continue

                    cand = None
                    # prioritas: baris berlabel harga
                    if _has_any(la_l, PRICE_HINTS):
                        cand = _prefer_price_from_line(la_s)
                    # alternatif: angka kecil yang lolos aturan price
                    if not cand:
                        for m in re.finditer(r"[0-9][0-9\.,]*", la_s):
                            t = m.group(0)
                            if _looks_like_compact_date(t):
                                continue
                            if _ok_price_token(t):
                                try:
                                    v = NumberParser.parse_number(t) or 0
                                except Exception:
                                    v = 0
                                if 20 <= v <= MAX_PRICE_IDR:
                                    cand = t
                                    break
                    if cand:
                        chosen = cand
                        break
                if chosen:
                    row_price_s = chosen
                    continue
                else:
                    # belum dapat price; lanjut scan
                    continue

            # (3) date
            if row_kind is not None and row_price_s is not None and row_date_s is None:
                if parse_id_en_date(raw):
                    row_date_s = raw
                    continue

            # (4) amount — utamakan thousands formatted; hindari tanggal kompak
            if row_kind is not None and row_price_s is not None and row_date_s is not None and row_amt_s is None:
                ss = raw.strip()
                if _RE_BIG_INT.fullmatch(ss) and not _looks_like_compact_date(ss.replace(".", "").replace(",", "")):
                    row_amt_s = ss
                elif _RE_ANYNUM.fullmatch(ss):
                    # pilih angka terbesar yang bukan tanggal kompak & >= 1000 sebagai jumlah
                    try:
                        val = NumberParser.parse_number(ss) or 0
                    except Exception:
                        val = 0
                    if val >= 1000 and not _looks_like_compact_date(ss):
                        row_amt_s = ss

            # finalize row
            if row_kind and row_price_s and row_date_s and row_amt_s:
                try:
                    rows.append(self._push_row(row_kind, row_price_s, row_date_s, row_amt_s))
                except Exception as e:
                    logger.warning("Line parser failed: %s | data=(%s,%s,%s,%s)",
                                   e, row_kind, row_price_s, row_date_s, row_amt_s)
                # reset untuk menangkap baris berikutnya (multi-row table)
                row_kind = row_price_s = row_date_s = row_amt_s = None

        if not rows:
            logger.debug("Stacked-cell parser found 0 rows. Window after header: %s",
                         self.lines[header_idx + 1: header_idx + 11])
        return rows, header_idx

    def _parse_transactions_window_fallback(self, window: List[str]) -> Optional[Dict[str, Any]]:
        if not window:
            return None

        kind = None
        for s in window:
            ls = (s or "").lower()
            if "buy" in ls or "pembelian" in ls:
                kind = "buy"; break
            if "sell" in ls or "penjualan" in ls:
                kind = "sell"; break
            if "transfer" in ls or "pengalihan" in ls:
                kind = "transfer"; break

        price_s = None
        for s in window:
            cand = _prefer_price_from_line(s)
            if cand:
                price_s = cand
                break

        date_s = None
        for s in window:
            if parse_id_en_date(s):
                date_s = s.strip()
                break

        amount_s = None
        max_val = -1
        for s in window:
            ss = (s or "").strip()
            if _RE_BIG_INT.fullmatch(ss) and not _looks_like_compact_date(ss.replace(".", "").replace(",", "")):
                amount_s = ss
                break
            if _RE_ANYNUM.fullmatch(ss):
                try:
                    val = NumberParser.parse_number(ss) or 0
                except Exception:
                    val = 0
                if val >= 1000 and not _looks_like_compact_date(ss) and val > max_val:
                    max_val = val
                    amount_s = ss

        if kind and price_s and date_s and amount_s:
            try:
                return self._push_row(kind, price_s, date_s, amount_s)
            except Exception as e:
                logger.warning("Window fallback failed: %s", e)
        return None

    def _parse_transactions_loose_lines(self) -> List[Dict[str, Any]]:
        """
        Tanpa header: hanya ambil jika baris mengandung konteks harga.
        Ini mencegah entri palsu (price=1, amount=YYYYMMDD).
        """
        rows: List[Dict[str, Any]] = []
        for row in self.lines:
            s = (row or "").strip()
            if not s:
                continue
            lo = s.lower()
            if not any(k in lo for k in ("buy", "sell", "pembelian", "penjualan", "transfer", "pengalihan")):
                continue

            # WAJIB ada konteks harga agar aman
            if not _has_any(lo, PRICE_HINTS):
                continue

            kind = "transfer"
            if "buy" in lo or "pembelian" in lo:
                kind = "buy"
            elif "sell" in lo or "penjualan" in lo:
                kind = "sell"

            price_s = _prefer_price_from_line(s)
            if not price_s:
                continue

            # date
            dm = _DATE_ANY.search(s)
            date_s = dm.group(0) if dm else None

            # amount
            amount_s = None
            tokens = re.findall(r"[0-9][0-9\.,]*", s)
            # utamakan thousands-formatted
            for t in tokens:
                if _RE_BIG_INT.fullmatch(t):
                    amount_s = t
                    break
            if not amount_s:
                max_val = -1
                for t in tokens:
                    if _looks_like_compact_date(t):
                        continue
                    if _RE_ANYNUM.fullmatch(t):
                        try:
                            v = NumberParser.parse_number(t) or 0
                        except Exception:
                            v = 0
                        if v >= 1000 and v > max_val:
                            max_val = v
                            amount_s = t

            if kind and price_s and amount_s:
                try:
                    rows.append(self._push_row(kind, price_s, date_s, amount_s))
                except Exception as e:
                    logger.warning("Loose-line row failed: %s | data=(%s,%s,%s,%s)",
                                   e, kind, price_s, date_s, amount_s)
        return rows
