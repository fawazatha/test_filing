# parser_non_idx.py
from __future__ import annotations
import os, re, json
import unicodedata
import pdfplumber
from typing import Dict, Any, Optional, List, Tuple

from src.common.log import get_logger
from src.common.datetime import MONTHS_EN, MONTHS_ID

from .base_parser import BaseParser
from .utils.number_parser import NumberParser
from .utils.name_cleaner import NameCleaner
from .utils.transaction_classifier import TransactionClassifier
from .utils.company_resolver import (
    load_symbol_to_name_from_file,
    build_reverse_map,
    resolve_symbol_from_emiten,
    normalize_company_name,
)

logger = get_logger(__name__)   

_MONTH: Dict[str, int] = {}
_MONTH.update(MONTHS_ID)
_MONTH.update(MONTHS_EN)

_DATE_RE = re.compile(r'tanggal\s*:\s*(\d{1,2})\s+([A-Za-zÀ-ÿ]+)\s+(\d{4})', re.IGNORECASE)

def _parse_tx_date_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    m = _DATE_RE.search(text)
    if not m:
        return None
    d, mon, y = m.groups()
    mm = _MONTH.get(mon.lower())
    return f"{int(y):04d}-{int(mm):02d}-{int(d):02d}" if mm else None


# Company map helpers
def _load_company_map(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def _symbol_keys(sym: Optional[str]) -> List[str]:
    s = (sym or "").upper()
    keys = {s}
    if s and not s.endswith(".JK"):
        keys.add(f"{s}.JK")
    if s.endswith(".JK"):
        keys.add(s[:-3])
    return list(keys)

def _estimate_last_close_price(sym: Optional[str], company_map: Dict[str, Any]) -> Optional[float]:
    for k in _symbol_keys(sym):
        meta = company_map.get(k)
        if meta and "last_close_price" in meta:
            try:
                return float(meta["last_close_price"])
            except Exception:
                pass
    return None


# Name helpers
def _title_case_holder(name: str) -> str:
    if not name:
        return name
    try:
        return NameCleaner.to_title_case_custom(name)
    except Exception:
        s = name.title()
        # perapihan umum
        s = re.sub(r'\bOf\b', 'of', s)
        s = re.sub(r'\bAnd\b', 'and', s)
        s = re.sub(r'\bPt\b', 'PT', s)
        s = re.sub(r'\bTbk\b', 'Tbk', s)
        s = re.sub(r'\bLtd\b', 'Ltd', s)
        s = re.sub(r'\bLimited\b', 'Limited', s)
        return s


# Downloads Meta
_DL_DEFAULT_PATH = os.getenv("DOWNLOADS_META_FILE", "data/downloaded_pdfs.json")

def _load_downloads_meta(path: Optional[str] = None) -> List[Dict[str, Any]]:
    path = path or _DL_DEFAULT_PATH
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []

def _basename(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    try:
        return os.path.basename(str(s))
    except Exception:
        return None

def _stem(s: Optional[str]) -> Optional[str]:
    b = _basename(s)
    if not b:
        return None
    root, _ = os.path.splitext(b)
    return root

def _resolve_dl_ctx(downloads_meta: List[Dict[str, Any]], filename: str) -> Dict[str, Any]:
    fn = (filename or "").strip()
    base = _basename(fn)
    st   = _stem(fn)

    # exact filename
    for row in downloads_meta:
        if not isinstance(row, dict):
            continue
        if _basename(row.get("filename")) == base:
            return row

    # url basename
    for row in downloads_meta:
        if not isinstance(row, dict):
            continue
        if _basename(row.get("url")) == base:
            return row

    # stem
    for row in downloads_meta:
        if not isinstance(row, dict):
            continue
        if _stem(row.get("filename")) == st or _stem(row.get("url")) == st:
            return row

    return {}


class NonIDXParser(BaseParser):
    _CORP_STOPWORDS = {"pt", "p.t", "perseroan", "terbatas", "tbk", "tbk.", "tbk,", "(tbk"}
    _TOKEN_SPLIT = re.compile(r"[^a-z0-9]+", re.UNICODE)

    def __init__(
        self,
        pdf_folder: str = "downloads/non-idx-format",
        output_file: str = "data/parsed_non_idx_output.json",
        announcement_json: str = "data/idx_announcements.json",
    ):
        super().__init__(
            pdf_folder=pdf_folder,
            output_file=output_file,
            announcement_json=announcement_json,
        )
        # Label parser ini
        self.parser_type = "non_idx"

        self.excluded_names = {"Masyarakat lainnya yang dibawah 5%"}
        self._symbol_to_name: Optional[Dict[str, str]] = None
        self._rev_company_map: Optional[Dict[str, List[str]]] = None
        self._debug_trace = os.getenv("COMPANY_RESOLVE_DEBUG", "0") == "1"
        self._synonym_enable = os.getenv("NONIDX_RESOLVE_SYNONYM_ENABLE", "1") != "0"

    @staticmethod
    def _normalize_symbol(sym: str) -> str:
        s = (sym or "").strip().upper()
        return s[:-3] if s.endswith(".JK") else s

    @classmethod
    def _normalize_name(cls, s: str) -> str:
        s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")
        s = s.lower()
        tokens = [t for t in cls._TOKEN_SPLIT.split(s) if t]
        tokens = [t for t in tokens if t not in cls._CORP_STOPWORDS]
        return " ".join(tokens)

    def _ensure_company_maps(self):
        if self._symbol_to_name is not None and self._rev_company_map is not None:
            return
        try:
            symbol_to_name = load_symbol_to_name_from_file() or {}
        except Exception as e:
            logger.error(f"Failed to load company_map.json: {e}")
            symbol_to_name = {}

        self._symbol_to_name = symbol_to_name
        self._rev_company_map = build_reverse_map(symbol_to_name)

        logger.info(
            "[company_map] file=%s symbols=%d reverse_keys=%d",
            os.getenv("COMPANY_MAP_FILE", "data/company/company_map.json"),
            len(symbol_to_name),
            len(self._rev_company_map or {}),
        )

        if self._debug_trace:
            probe = normalize_company_name("PT SUMBER ENERGI ANDALAN TBK")
            exists = probe in (self._rev_company_map or {})
            logger.info("[company_map] probe_key='%s' present=%s", probe, exists)

    # Entry point
    def parse_single_pdf(self, filepath: str, filename: str, pdf_mapping: Dict[str, Any],) -> Optional[List[Dict[str, Any]]]:
        """
        Parse a single Non-IDX-format insider report into a list of
        normalized row-level filings.

        Parser alert codes used at this stage:
          - table_not_found   (fatal, not_inserted)
          - parse_exception   (fatal, not_inserted)
          - company_resolve_ambiguous (warning, inserted; per row)
        """
        ann_ctx = (pdf_mapping or {}).get(filename, {})

        # Download meta: used to fill 'source' and 'timestamp' from downloader.
        downloads_meta = _load_downloads_meta()
        dl_ctx = _resolve_dl_ctx(downloads_meta, filename)
        self._current_alert_context = ann_ctx or {}

        try:
            with pdfplumber.open(filepath) as pdf:
                all_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

                title_line, reporter_name = self._extract_metadata(all_text)
                emiten_name = self._extract_emiten_name(all_text)

                self._ensure_company_maps()

                last_page = pdf.pages[-1]
                table = self._extract_last_page_table(last_page)
                if not table or len(table) < 2:
                    self._parser_fail(
                        code="table_not_found",
                        filename=filename,
                        reasons=[
                            {
                                "scope": "parser",
                                "code": "table_not_found",
                                "message": "No compatible transaction table was found in the document.",
                                "details": {
                                    "announcement": ann_ctx,
                                    "downloads_meta": dl_ctx,
                                },
                            }
                        ],
                    )
                    self._blocked_already_logged = True
                    return None

                data_rows = self._process_table_rows( table=table, all_text=all_text, title_line=title_line, emiten_name=emiten_name, source_name=filename,)

                filtered_rows = [
                    entry for entry in data_rows
                    if entry.get("holder_name") not in self.excluded_names
                    and "masyarakat lainnya" not in (entry.get("holder_name") or "").lower()
                ]

                # Dates
                tx_date = _parse_tx_date_from_text(all_text)

                # Pull URL & timestamp from downloads meta
                dl_url = dl_ctx.get("url")
                dl_ts  = dl_ctx.get("timestamp") 

                for e in filtered_rows:
                    # SOURCE & TIMESTAMP (FROM DOWNLOADED_PDFS.JSON)
                    if dl_url:
                        e["source"] = dl_url
                    # fallback timestamp: downloaded meta -> tanggal di teks
                    if dl_ts:
                        e["timestamp"] = dl_ts
                    elif tx_date:
                        e["timestamp"] = tx_date

                    # Perapihan holder
                    if e.get("holder_name"):
                        e["holder_name"] = _title_case_holder(e["holder_name"])

                    # amount_transaction jika kosong (berdasarkan holding_before/after)
                    if not e.get("amount_transaction"):
                        hb, ha = e.get("holding_before"), e.get("holding_after")
                        if isinstance(hb, (int, float)) and isinstance(ha, (int, float)):
                            try:
                                e["amount_transaction"] = abs(int(float(ha)) - int(float(hb)))
                            except Exception:
                                pass

                    # Tentukan type bila kosong
                    hb, ha = e.get("holding_before"), e.get("holding_after")
                    tx_type = e.get("transaction_type")
                    if not tx_type and isinstance(hb, (int, float)) and isinstance(ha, (int, float)):
                        tx_type = "buy" if ha > hb else "sell"
                        e["transaction_type"] = tx_type

                    # Harga: gunakan yang ada di dokumen; fallback ke 0 (jangan pakai company_map)
                    price_final = None
                    try:
                        raw_price = e.get("price")
                        if raw_price not in (None, ""):
                            price_final = float(str(raw_price).replace(",", "").strip())
                    except Exception:
                        price_final = None
                    if price_final is None:
                        price_final = 0.0

                    # Gunakan tx_date; kalau kosong, potong tanggal dari dl_ts (YYYY-MM-DD)
                    tx_date_final = tx_date or (str(dl_ts)[:10] if dl_ts else None)

                    e["price_transaction"] = [{
                        "date": tx_date_final,
                        "type": e.get("transaction_type"),
                        "price": price_final,
                        "amount_transacted": e.get("amount_transaction"),
                    }]

                    if e.get("amount_transaction"):
                        try:
                            e["price"] = price_final
                            e["transaction_value"] = float(price_final) * float(e["amount_transaction"])
                        except Exception:
                            pass

                return filtered_rows or None

        except Exception as e:
            logger.error(f"Error parsing {filename}: {e}")
            self._parser_fail(
                code="parse_exception",
                filename=filename,
                reasons=[
                    {
                        "scope": "parser",
                        "code": "parse_exception",
                        "message": f"Unexpected error while parsing the document: {e}",
                        "details": {
                            "announcement": ann_ctx,
                            "downloads_meta": dl_ctx,
                        },
                    }
                ],
            )
            self._blocked_already_logged = True
            return None

    # PDF helpers
    def _extract_last_page_table(self, last_page) -> Optional[List[List[str]]]:
        table_settings = {
            "vertical_strategy": "lines",
            "horizontal_strategy": "lines",
            "intersection_tolerance": 5,
            "snap_tolerance": 3,
            "join_tolerance": 3,
            "edge_min_length": 3,
            "min_words_vertical": 1,
            "min_words_horizontal": 1,
        }

        # Strategy 1: tuned settings + extract_table
        tbl = last_page.extract_table(table_settings=table_settings)
        if tbl and len(tbl) >= 2:
            return tbl

        # Strategy 2: tuned settings + extract_tables
        tables = last_page.extract_tables(table_settings=table_settings) or []
        tables = [t for t in tables if t and len(t) >= 2]
        if tables:
            tables.sort(key=lambda t: len(t), reverse=True)
            return tables[0]

        # Strategy 3: default extract_table
        tbl = last_page.extract_table()
        if tbl and len(tbl) >= 2:
            return tbl

        # Strategy 4: default extract_tables
        tables = last_page.extract_tables() or []
        tables = [t for t in tables if t and len(t) >= 2]
        if tables:
            tables.sort(key=lambda t: len(t), reverse=True)
            return tables[0]

        return None


    def _extract_metadata(self, text: str) -> Tuple[str, str]:
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        title_line = next(
            (line for line in lines if "LAPORAN KEPEMILIKAN EFEK" in line.upper()),
            ""
        )
        bae_line = next((line for line in lines if "BAE" in line.upper()), "")
        reporter_name = bae_line.split(":")[-1].strip() if ":" in bae_line else "UNKNOWN"
        return title_line, reporter_name


    def _extract_emiten_name(self, text: str) -> Optional[str]:
        lines = [l.strip() for l in text.splitlines() if l.strip()]
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

        m = re.search(r'(PT\s+.+?Tbk\.?)', text, flags=re.I)
        if m:
            return m.group(1).strip()
        return None


    # Company resolution
    def _resolve_symbol_from_emiten_local(self, emiten_name: Optional[str], full_text: str) -> Optional[str]:
        if not self._symbol_to_name or not self._rev_company_map:
            return None

        min_score = int(os.getenv("NONIDX_RESOLVE_MIN_SCORE", "88"))
        query = emiten_name or ""
        norm_query = normalize_company_name(query)

        sym, key_used, tried = resolve_symbol_from_emiten(
            query,
            symbol_to_name=self._symbol_to_name,
            rev_map=self._rev_company_map,
            fuzzy=True,
            min_score=min_score,
        )

        if self._debug_trace:
            logger.info(
                "[nonidx-resolve] query='%s' norm='%s' sym=%s key_used='%s' tried=%s",
                query, norm_query, sym, key_used, tried,
            )

        if sym:
            base = sym[:-3] if sym.endswith(".JK") else sym
            return base

        m = re.search(r'(PT\s+.+?Tbk\.?)', full_text or "", flags=re.I)
        if m:
            alt = m.group(1)
            sym2, key2, tried2 = resolve_symbol_from_emiten(
                alt,
                symbol_to_name=self._symbol_to_name,
                rev_map=self._rev_company_map,
                fuzzy=True,
                min_score=min_score,
            )
            if self._debug_trace:
                logger.info(
                    "[nonidx-resolve] alt='%s' sym=%s key_used='%s' tried=%s",
                    alt, sym2, key2, tried2
                )
            if sym2:
                base2 = sym2[:-3] if sym2.endswith(".JK") else sym2
                return base2

        candidates = set(re.findall(r'\b([A-Z]{3,4})\b', full_text or ""))
        for cand in candidates:
            if cand in self._symbol_to_name or f"{cand}.JK" in self._symbol_to_name:
                if self._debug_trace:
                    logger.info("[nonidx-resolve] token-scan hit cand=%s", cand)
                return cand

        if self._debug_trace:
            logger.info(
                "[nonidx-resolve-debug] symbol resolution failed: query='%s' norm='%s' min_score=%s",
                query,
                norm_query,
                min_score,
            )
        return None

    # Row processing
    def _coerce_dash_zero(self, s: Any, as_percentage: bool = False):
        txt = (str(s or "")).strip()
        if txt in {"-", "–", "—", ""}:
            return 0.0 if as_percentage else 0
        try:
            if as_percentage:
                return NumberParser.parse_percentage(txt)
            val = NumberParser.parse_number(txt, is_percentage=False)
            return val if val is not None else 0
        except Exception:
            return 0.0 if as_percentage else 0

    def _process_table_rows(self,
                            table: List[List[str]],
                            all_text: str,
                            title_line: str,
                            emiten_name: Optional[str],
                            source_name: str) -> List[Dict[str, Any]]:
        data_rows: List[Dict[str, Any]] = []

        for i, row in enumerate(table):
            if not row:
                continue

            joined = " ".join((c or "").lower() for c in row)
            if any(k in joined for k in ["sebelum", "sesudah", "jumlah", "persen", "persentase",
                                         "percentage", "pemilikan %"]):
                continue
            if "total" in joined:
                continue

            if len(row) < 5:
                continue

            try:
                result = self._process_single_row(
                    row=row,
                    all_text=all_text,
                    title_line=title_line,
                    source_name=source_name,
                    emiten_name=emiten_name
                )
                if result:
                    data_rows.append(result)
                    logger.info(f"parsed row: {result['holder_name']}")
            except Exception as e:
                logger.warning(f"Row parse error: {e}")
                continue

        return data_rows


    def _process_single_row(self, row: List[str], all_text: str, title_line: str, source_name: str, emiten_name: Optional[str]) -> Optional[Dict[str, Any]]:
        safe_row = [(c or "").strip() for c in row]
        if len(safe_row) < 5:
            return None

        if any("total" in (c or "").lower() for c in safe_row):
            return None

        holder_name_raw = safe_row[1] if len(safe_row) > 1 else ""
        if not holder_name_raw:
            return None
        if "masyarakat lainnya" in holder_name_raw.lower():
            return None

        cols = safe_row[-4:] if len(safe_row) >= 4 else ["", "", "", ""]

        holding_before = self._coerce_dash_zero(cols[0], as_percentage=False)
        holding_after  = self._coerce_dash_zero(cols[1], as_percentage=False)
        pct_before     = self._coerce_dash_zero(cols[2], as_percentage=True)
        pct_after      = self._coerce_dash_zero(cols[3], as_percentage=True)

        try:
            if float(holding_before) == float(holding_after) and float(pct_before) == float(pct_after):
                return None
        except Exception:
            pass

        holder_type = NameCleaner.classify_holder_type(holder_name_raw)
        holder_name = NameCleaner.clean_holder_name(holder_name_raw, holder_type)

        if not NameCleaner.is_valid_holder(holder_name):
            return None

        share_pct_transaction = round(abs(float(pct_after) - float(pct_before)), 3)

        # Classify tx type from text/percentages (prelim; tags will be recomputed canonically)
        tx_type, _prelim = TransactionClassifier.classify_transaction_type(
            all_text, float(pct_before), float(pct_after)
        )


        # Build base filing
        filing: Dict[str, Any] = {
            "title": title_line.strip(),
            "body": all_text.strip(),
            "source": source_name,   
            "timestamp": None,      
            "tags": [],              
            "symbol": None,
            "transaction_type": tx_type,
            "holder_type": holder_type,
            "holding_before": holding_before,
            "holding_after": holding_after,
            "share_percentage_before": pct_before,
            "share_percentage_after": pct_after,
            "share_percentage_transaction": share_pct_transaction,
            "amount_transaction": abs(int(float(holding_after)) - int(float(holding_before))),
            "holder_name": holder_name,
            "price": None,
            "transaction_value": None,
            "price_transaction": None,
            "UID": None,
        }

        # Company symbol (best effort)
        try:
            sym = self._resolve_symbol_from_emiten_local(emiten_name, all_text)
            if sym:
                filing["symbol"] = sym
        except Exception as e:
            logger.debug(f"Local symbol resolution failed (emiten='{emiten_name}'): {e}")

        if not filing["symbol"]:
            try:
                em_norm_internal = self._normalize_name(emiten_name or "")
                em_norm_global = normalize_company_name(emiten_name or "")
                payload = {
                    "emiten": emiten_name,
                    "normalized_internal": em_norm_internal,
                    "normalized_global": em_norm_global,
                    "min_score": int(os.getenv("NONIDX_RESOLVE_MIN_SCORE", "88")),
                    "debug": self._debug_trace,
                }
                self._parser_warn(
                    code="company_resolve_ambiguous",
                    filename=source_name,
                    reasons=[
                        {
                            "scope": "parser",
                            "code": "company_resolve_ambiguous",
                            "message": (
                                "Issuer mapping is ambiguous or below confidence "
                                "threshold in NonIDX parser."
                            ),
                            "details": payload,
                        }
                    ],
                )
            except Exception:
                logger.warning("[alert] Symbol Not Resolved for %s (emiten='%s')", source_name, emiten_name,)

        # Standardized tags
        flags = TransactionClassifier.detect_flags_from_text(all_text)
        txns = [{"type": tx_type, "amount": filing["amount_transaction"] or 0}] if tx_type else []

        filing["tags"] = TransactionClassifier.compute_filings_tags(
            txns=txns,
            share_percentage_before=filing["share_percentage_before"],
            share_percentage_after=filing["share_percentage_after"],
            flags=flags,
        )

        return filing

    # Output
    def validate_parsed_data(self, data: List[Dict[str, Any]]) -> bool:
        return bool(data)

    def save_results(self, results: List[List[Dict[str, Any]]]):
        flattened_results: List[Dict[str, Any]] = []
        for result_list in results:
            if isinstance(result_list, list):
                flattened_results.extend(result_list)
            elif result_list:
                flattened_results.append(result_list)
        super().save_results(flattened_results)
