from __future__ import annotations
from typing import Dict, Any, Optional, List, Tuple
import os
import re
import pdfplumber

# Core/Base
from src.parser.core.base_parser import BaseParser

# Common Libs
from src.common.numbers import NumberParser
from src.common.log import get_logger
from src.common.strings import normalize_company_key as global_normalize_key

# Parser Utils
from src.parser.utils.name_cleaner import NameCleaner
from src.parser.utils.transaction_classifier import TransactionClassifier
from src.parser.utils.pdf_tables import extract_table_like
from src.parser.utils.company import CompanyService
from src.parser.utils.text_extractor import TextExtractor  # Import TextExtractor

logger = get_logger(__name__)

#- Heuristics for price extraction (non-IDX narrative)-
# allow up to 6 digits, optional decimals; reject thousands-formatted ints
_RE_BIG_INT = re.compile(r"^\d{1,3}(?:[.,]\d{3})+$")           # e.g. 14.838.000
_RE_PRICE   = re.compile(r"^\d{1,6}(?:[.,]\d{1,2})?$")         # e.g. 55, 198, 10150, 75.5

# Month tokens (EN/ID) to avoid picking day parts of dates as "prices"
_MONTH_WORD = (
    r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
    r"Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?|"
    r"Jan(?:uari)?|Feb(?:ruari)?|Mar(?:et)?|Apr(?:il)?|Mei|Jun(?:i)?|Jul(?:i)?|"
    r"Agu(?:stus)?|Sep(?:tember)?|Okt(?:ober)?|Nov(?:ember)?|Des(?:ember)?)"
)
_MONTH_RE = re.compile(rf"\b{_MONTH_WORD}\b", re.IGNORECASE)
_DATE_ANY = re.compile(
    rf"\b\d{{1,2}}\s+{_MONTH_WORD}(?:\s+\d{{2,4}})?\b|"
    rf"\b\d{{1,2}}[/-]\d{{1,2}}[/-]\d{{2,4}}\b",
    re.IGNORECASE,
)

def _date_spans_in_text(s: str) -> List[tuple[int, int]]:
    spans: List[tuple[int, int]] = []
    for m in _DATE_ANY.finditer(s or ""):
        spans.append(m.span())
    return spans

def _span_contains(idx: int, spans: List[tuple[int, int]]) -> bool:
    for a, b in spans:
        if a <= idx < b:
            return True
    return False

def _prefer_price_from_line(line: str) -> Optional[str]:
    if not line:
        return None
    s = line.strip()
    lwr = s.lower()
    date_spans = _date_spans_in_text(s)

    has_price_hint = ("harga transaksi" in lwr) or ("transaction price" in lwr) or ("harga:" in lwr)
    is_amount_line = ("jumlah saham" in lwr) or ("number of shares" in lwr)

    tokens = list(re.finditer(r"[0-9][0-9\.,]*", s))
    if not tokens:
        return None

    def score(tok: str, start: int) -> int:
        sc = 0
        # jangan ambil token yang nempel tanggal
        if _span_contains(start, date_spans):
            return -999
        # hindari angka tepat sebelum/ sesudah kata bulan
        after = s[start:start + 12]
        if _MONTH_RE.search(after):
            return -998

        if has_price_hint:
            sc += 6
        if ("rp" in lwr) or ("idr" in lwr):
            sc += 2
        if ("," in tok or "." in tok):
            sc += 1

        try:
            val = NumberParser.parse_number(tok) or 0
            # kurangi skor kalau kemungkinan hari kalender (≤31) tanpa hint
            if not has_price_hint and val <= 31:
                sc -= 3
            # angka sangat besar cenderung amount
            if val > 100_000:
                sc -= 4
        except Exception:
            pass

        # penalti kuat jika barisnya jelas-jelas baris jumlah saham
        if is_amount_line:
            sc -= 8

        return sc

    best_tok, best_sc = None, -999
    for m in tokens:
        t = m.group(0)
        # kandidat yang diizinkan sebagai harga:
        is_candidate = (
            _RE_PRICE.fullmatch(t) or           # 55 / 1250 / 75.5
            _RE_BIG_INT.fullmatch(t) or         # 1.370 / 2.000 / 1,485
            (has_price_hint and _RE_ANYNUM.fullmatch(t))
        )
        if not is_candidate:
            continue

        sc = score(t, m.start())
        if sc > best_sc:
            best_sc, best_tok = sc, t

    return best_tok



def _clean_cell(s: Any) -> str:
    return (str(s or "").replace("\n", " ").strip())


class NonIDXParser(BaseParser):
    """
    Parser for non-IDX-format PDFs.
    Refactored to:
    - Use common NumberParser directly
    - Use centralized TransactionClassifier.validate_direction
    - Break down monolithic _parse_row into smaller helpers
    - Extract "purpose" field
    - Try to detect narrative transaction price and produce price_transaction[]
    - Check price vs last_close_price (±50% alert)
    """

    EXCLUDED_ROWS = {"Masyarakat lainnya yang dibawah 5%"}  # exact blacklist
    _HDR_HINTS = ("sebelum", "sesudah", "jumlah", "persen", "persentase", "percentage", "pemilikan %")

    def __init__(
        self,
        pdf_folder: str = "downloads/non-idx-format",
        output_file: str = "data/parsed_non_idx_output.json",
        announcement_json: str = "data/idx_announcements.json",
    ):
        super().__init__(
            pdf_folder,
            output_file,
            announcement_json,
            alerts_file=os.getenv("ALERTS_NON_IDX", "alerts/alerts_non_idx.json"),
            alerts_not_inserted_file=os.getenv("ALERTS_NOT_INSERTED_NON_IDX", "alerts/alerts_not_inserted_non_idx.json"),
        )
        self.company = CompanyService()
        self._debug_trace = os.getenv("COMPANY_RESOLVE_DEBUG", "0") in {"1", "true", "on"}

    # == Entry point ==
    def parse_single_pdf(
        self, filepath: str, filename: str, pdf_mapping: Dict[str, Any]
    ) -> Optional[List[Dict[str, Any]]]:

        ann_ctx = (pdf_mapping or {}).get(filename, {})

        try:
            with pdfplumber.open(filepath) as pdf:
                all_text = "\n".join((_t or "") for _t in (p.extract_text() for p in pdf.pages if p.extract_text()))
                if not all_text.strip():
                    self.alert_manager_not_inserted.log_alert(filename, "no_text_extracted", {"announcement": ann_ctx})
                    return None

                # Use TextExtractor for keyword searching
                ex = TextExtractor(all_text)

                title_line, _ = self._extract_metadata(all_text)
                emiten_name = self._extract_emiten_name(all_text)

                symbol = self.company.resolve_symbol(emiten_name, all_text, min_score_env="88")
                if not symbol:
                    self._log_symbol_alert(filename, emiten_name)

                last_page = pdf.pages[-1]
                table = extract_table_like(last_page)
                if not table:
                    self.alert_manager_not_inserted.log_alert(filename, "No Table Found", {"announcement": ann_ctx})
                    return None

                rows: List[Dict[str, Any]] = []
                # Try to grab a document-level narrative price once (shared by rows)
                doc_price = self._extract_doc_level_price(ex)

                for row_cells in self._iter_data_rows(table):
                    parsed = self._parse_row(
                        row_cells=row_cells,
                        all_text=all_text,
                        extractor=ex,   # pass the extractor
                        title_line=title_line,
                        source_name=filename,
                        doc_symbol=symbol,
                        doc_price=doc_price,  # pass doc-level price
                    )
                    if parsed:
                        # Check price deviation vs last_close if we had a price
                        self._check_price_deviation_vs_last_close(parsed, filename, ann_ctx)
                        rows.append(parsed)

                filtered = [r for r in rows if self._is_valid_filing(r)]
                return filtered or None

        except Exception as e:
            logger.error(f"Error parsing {filename}: {e}", exc_info=True)
            self.alert_manager_not_inserted.log_alert(
                filename, "parsing_error", {"message": str(e), "announcement": ann_ctx}
            )
            return None

    # == PDF metadata ==
    def _extract_metadata(self, text: str) -> Tuple[str, str]:
        lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
        title_line = next((ln for ln in lines if "LAPORAN KEPEMILIKAN EFEK" in ln.upper()), "")
        bae_line = next((ln for ln in lines if "BAE" in ln.upper()), "")
        reporter_name = bae_line.split(":")[-1].strip() if ":" in bae_line else "UNKNOWN"
        return title_line, reporter_name

    def _extract_emiten_name(self, text: str) -> Optional[str]:
        lines = [l.strip() for l in (text or "").splitlines() if l.strip()]
        patterns = [
            r'^\s*nama\s+emiten\s*[:\-]\s*(.+)$',
            r'^\s*emiten\s*[:\-]\s*(.+)$',
            r'^\s*nama\s+perusahaan\s*[:\-]\s*(.+)$',
            r'^\s*perseroan\s*[:\-]\s*(.+)$',
            r'^\s*issuer\s*[:\-]\s*(.+)$',
        ]
        for line in lines:
            for pat in patterns:
                m = re.search(pat, line, flags=re.I)
                if m:
                    name = m.group(1).strip().strip('“”"[]().')
                    name = re.sub(r'\(\s*"?perseroan"?\s*\)', '', name, flags=re.I).strip()
                    return name

        m = re.search(r'(PT\s+.+?Tbk\.?)', text or "", flags=re.I)
        return m.group(1).strip() if m else None

    # == Row iteration ==
    def _iter_data_rows(self, table: List[List[str]]):
        for raw in table:
            row = [_clean_cell(c) for c in (raw or [])]
            if not row or len(row) < 5:
                continue
            joined = " ".join((c or "").lower() for c in row)
            if any(h in joined for h in self._HDR_HINTS):
                continue
            if "total" in joined:
                continue
            yield row

    # == Row parsing (REFACTORED) ==
    def _parse_row(
        self,
        row_cells: List[str],
        all_text: str,
        extractor: TextExtractor,  # <-- Added extractor
        title_line: str,
        source_name: str,
        doc_symbol: Optional[str],
        doc_price: Optional[float],   # <-- Added doc-level narrative price
    ) -> Optional[Dict[str, Any]]:
        """
        Main parsing coordinator for a single row.
        Delegates to smaller helper methods.
        """

        holder_name_raw, data = self._extract_row_data(row_cells)
        if not holder_name_raw:
            return None

        holder_type = NameCleaner.classify_holder_type(holder_name_raw)
        holder_name = NameCleaner.clean_holder_name(holder_name_raw, holder_type)
        if not NameCleaner.is_valid_holder(holder_name):
            return None

        data["holder_type"] = holder_type
        data["holder_name"] = holder_name

        if data["holding_before"] == data["holding_after"] and \
           data["share_percentage_before"] == data["share_percentage_after"]:
            return None

        tx_type, prelim_tags = TransactionClassifier.classify_transaction_type(
            all_text, data["share_percentage_before"], data["share_percentage_after"]
        )
        data["transaction_type"] = tx_type

        if tx_type in {"buy", "sell"}:
            ok, reason = TransactionClassifier.validate_direction(
                data["share_percentage_before"], data["share_percentage_after"], tx_type
            )
            if not ok:
                logger.debug("drop row: %s", reason)
                return None

        # Purpose from the full text (best-effort)
        data["purpose"] = (
            extractor.find_value_after_keyword("Tujuan Transaksi")
            or extractor.find_value_after_keyword("Purposes of the transaction")
            or extractor.find_value_after_keyword("Purpose of the Transaction")
            or ""
        ).strip()

        filing = self._build_filing_dict(
            data=data,
            title=title_line,
            source=source_name,
            symbol=doc_symbol,
            text=all_text,
        )

        #- NEW: attach price & price_transaction from narrative price-
        if doc_price and tx_type in {"buy", "sell"} and (filing.get("amount_transaction") or 0) > 0:
            amt = int(filing["amount_transaction"])
            filing["price"] = float(doc_price)
            filing["transaction_value"] = float(doc_price) * amt
            # list-of-objects format requested
            filing["price_transaction"] = [
                {
                    "date": None,  # non-IDX PDFs rarely provide per-transaction date per row
                    "type": tx_type,
                    "price": float(doc_price),
                    "amount_transacted": amt,
                }
            ]
        else:
            filing["price"] = filing.get("price") or None
            filing["transaction_value"] = filing.get("transaction_value") or None
            filing["price_transaction"] = filing.get("price_transaction") or []

        return filing

    def _extract_row_data(self, row: List[str]) -> Tuple[Optional[str], Dict[str, Any]]:
        """Extracts holder name and numerical data from table cells."""
        holder_name_raw = row[1] if len(row) > 1 else ""
        if not holder_name_raw or "masyarakat lainnya" in holder_name_raw.lower():
            return None, {}

        cols = row[-4:] if len(row) >= 4 else ["", "", "", ""]

        try:
            hb = NumberParser.parse_number(cols[0])
            ha = NumberParser.parse_number(cols[1])
            pb = NumberParser.parse_percentage(cols[2])
            pa = NumberParser.parse_percentage(cols[3])
        except Exception:
            hb, ha, pb, pa = 0, 0, 0.0, 0.0

        share_pct_tx = round(abs(pa - pb), 6)

        data = {
            "holding_before": hb,
            "holding_after": ha,
            "share_percentage_before": pb,
            "share_percentage_after": pa,
            "share_percentage_transaction": share_pct_tx,
            "amount_transaction": abs(int(ha) - int(hb)),
        }
        return holder_name_raw, data

    def _build_filing_dict(self, data: Dict, title: str, source: str, symbol: Optional[str], text: str) -> Dict[str, Any]:
        """Assembles the final filing dictionary and computes tags."""

        filing: Dict[str, Any] = {
            "title": title.strip(),
            "body": text.strip(),
            "source": source,
            "timestamp": None,
            "symbol": symbol,
            "price": None,
            "transaction_value": None,
            "price_transaction": None,  # will become list[]
            "UID": None,
            **data,
        }

        flags = TransactionClassifier.detect_flags_from_text(text)
        tx_type = filing["transaction_type"]
        txns = [{"type": tx_type, "amount": filing["amount_transaction"] or 0}] if tx_type else []

        filing["tags"] = TransactionClassifier.compute_filings_tags(
            txns=txns,
            share_percentage_before=filing["share_percentage_before"],
            share_percentage_after=filing["share_percentage_after"],
            flags=flags,
        )

        return filing

    def _is_valid_filing(self, r: Dict[str, Any]) -> bool:
        """Final check to filter out excluded or noisy rows."""
        holder = (r.get("holder_name") or "").lower()
        if holder in self.EXCLUDED_ROWS:
            return False
        if "masyarakat lainnya" in holder:
            return False
        return True

    # == Utilities ==
    def _log_symbol_alert(self, source_name: str, emiten_name: Optional[str]) -> None:
        """Helper to log a 'Symbol Not Resolved' alert."""
        try:
            self.alert_manager.log_alert(
                source_name,
                "Symbol Not Resolved",
                {
                    "emiten": emiten_name,
                    "normalized_global": global_normalize_key(emiten_name or ""),
                    "min_score": int(os.getenv("NONIDX_RESOLVE_MIN_SCORE", "88")),
                    "debug": self._debug_trace,
                },
            )
        except Exception:
            logger.warning("[alert] Symbol Not Resolved for %s (emiten='%s')", source_name, emiten_name)

    # == Output ==
    def validate_parsed_data(self, data: List[Dict[str, Any]]) -> bool:
        """Validate that the result is a non-empty list."""
        return isinstance(data, list) and bool(data)

    def save_results(self, results: List[Any]):
        """Flatten results (which are lists of dicts) and save."""
        flattened: List[Dict[str, Any]] = []
        for item in results:
            if isinstance(item, list):
                flattened.extend(item)
            elif isinstance(item, dict):
                flattened.append(item)

        super().save_results(flattened)

    #- NEW: helpers for doc-level price + deviation check-
    def _extract_doc_level_price(self, ex: TextExtractor) -> Optional[float]:
        """
        Many non-IDX PDFs mention a single 'Harga Transaksi' in narrative.
        We scan all lines and pick the best candidate once.
        """
        best: Optional[str] = None
        best_sc = -999
        for ln in ex.lines or []:
            tok = _prefer_price_from_line(ln)
            if not tok:
                continue
            # score again with simple line priority (contains harga/price)
            sc = 0
            L = (ln or "").lower()
            if ("harga" in L and "transaksi" in L) or ("transaction price" in L):
                sc += 3
            if ("rp" in L) or ("idr" in L):
                sc += 1
            if sc > best_sc:
                best_sc, best = sc, tok
        if best is None:
            return None
        try:
            return float(NumberParser.parse_number(best))
        except Exception:
            return None

    def _get_last_close(self, symbol: Optional[str]) -> Optional[float]:
        if not symbol:
            return None
        try:
            if hasattr(self.company, "get_last_close"):
                return self.company.get_last_close(symbol)
            # fallback: if CompanyService exposes a dict meta
            meta = getattr(self.company, "price_meta", None)
            if isinstance(meta, dict):
                lc = (meta.get(symbol) or {}).get("last_close_price")
                return float(lc) if lc is not None else None
        except Exception:
            return None
        return None

    def _check_price_deviation_vs_last_close(
        self,
        filing: Dict[str, Any],
        filename: str,
        ann_ctx: Dict[str, Any],
    ) -> None:
        """
        Compare each price in price_transaction to last_close_price; if deviation > 50%, alert.
        """
        symbol = filing.get("symbol")
        last_close = self._get_last_close(symbol)
        if not last_close or last_close <= 0:
            return

        pts = filing.get("price_transaction") or []
        for item in pts:
            price = item.get("price") or 0
            if not price or price <= 0:
                continue
            dev = abs(price - last_close) / float(last_close)
            if dev > 0.5:
                self.alert_manager.log_alert(
                    filename,
                    "price_deviation_gt_50",
                    {
                        "symbol": symbol,
                        "last_close_price": last_close,
                        "observed_price": price,
                        "deviation_pct": round(dev * 100.0, 2),
                        "date": item.get("date"),
                        "type": item.get("type"),
                        "amount_transacted": item.get("amount_transacted"),
                        "announcement": ann_ctx,
                    },
                )
