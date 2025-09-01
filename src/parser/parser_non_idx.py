from __future__ import annotations

import os
import re
import logging
import unicodedata
from typing import Dict, Any, Optional, List, Tuple

import pdfplumber

from .core.base_parser import BaseParser
from .utils.number_parser import NumberParser
from .utils.name_cleaner import NameCleaner
from .utils.transaction_classifier import TransactionClassifier
from .utils.company_resolver import (
    load_symbol_to_name_from_file,
    build_reverse_map,
    resolve_symbol_from_emiten,
    normalize_company_name,
)
from .utils.company_resolver import KNOWN_EMITEN_SYNONYMS as _RAW_EMITEN_SYNONYMS

logger = logging.getLogger(__name__)


class NonIDXParser(BaseParser):
    _CORP_STOPWORDS = {
        "pt", "p.t", "perseroan", "terbatas", "tbk", "tbk.", "tbk,", "(tbk"
    }
    _TOKEN_SPLIT = re.compile(r"[^a-z0-9]+", re.UNICODE)

    def __init__(self,
                 pdf_folder: str = "downloads/non-idx-format",
                 output_file: str = "data/parsed_non_idx_output.json",
                 announcement_json: str = "data/idx_announcements.json"):
        super().__init__(pdf_folder, output_file, announcement_json)
        self.alert_manager.alert_file = "alerts/alerts_non_idx.json"

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

    def parse_single_pdf(self, filepath: str, filename: str, pdf_mapping: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        ann_ctx = (pdf_mapping or {}).get(filename, {})
        try:
            with pdfplumber.open(filepath) as pdf:
                all_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

                title_line, reporter_name = self._extract_metadata(all_text)
                emiten_name = self._extract_emiten_name(all_text)

                self._ensure_company_maps()

                last_page = pdf.pages[-1]
                table = self._extract_last_page_table(last_page)
                if not table or len(table) < 2:
                    self.alert_manager_not_inserted.log_alert(
                        filename, "No Table Found", {"announcement": ann_ctx}
                    )
                    self._blocked_already_logged = True
                    return None

                data_rows = self._process_table_rows(table, all_text, title_line, emiten_name, filename)

                filtered_rows = [
                    entry for entry in data_rows
                    if entry.get("holder_name") not in self.excluded_names
                    and "masyarakat lainnya" not in (entry.get("holder_name") or "").lower()
                ]
                return filtered_rows or None

        except Exception as e:
            logger.error(f"Error parsing {filename}: {e}")
            self.alert_manager_not_inserted.log_alert(
                filename, "parsing_error", {"message": str(e), "announcement": ann_ctx}
            )
            self._blocked_already_logged = True
            return None

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

        tbl = last_page.extract_table(table_settings=table_settings)
        if tbl and len(tbl) >= 2:
            return tbl

        tables = last_page.extract_tables(table_settings=table_settings) or []
        tables = [t for t in tables if t and len(t) >= 2]
        if tables:
            tables.sort(key=lambda t: len(t), reverse=True)
            return tables[0]

        tbl = last_page.extract_table()
        if tbl and len(tbl) >= 2:
            return tbl

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
                query, norm_query, sym, key_used, tried
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

        if self._synonym_enable:
            syn = _RAW_EMITEN_SYNONYMS.get(norm_query)
            if syn:
                if self._debug_trace:
                    logger.info("[nonidx-resolve] synonym-fallback norm='%s' -> %s", norm_query, syn)
                return syn

        try:
            if self._debug_trace:
                self.alert_manager.log_alert(
                    "symbol_resolve_trace",
                    f"symbol_not_resolved (min_score={min_score})",
                    {"query": query, "normalized": norm_query}
                )
        except Exception:
            pass
        return None

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

    def _process_single_row(self,
                            row: List[str],
                            all_text: str,
                            title_line: str,
                            source_name: str,
                            emiten_name: Optional[str]) -> Optional[Dict[str, Any]]:

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

        share_pct_transaction = round(abs(float(pct_after) - float(pct_before)), 3)
        transaction_type, tags = TransactionClassifier.classify_transaction_type(
            all_text, float(pct_before), float(pct_after)
        )

        filing: Dict[str, Any] = {
            "title": title_line.strip(),
            "body": all_text.strip(),
            "source": source_name,
            "timestamp": None,
            "tags": tags,
            "symbol": None,
            "transaction_type": transaction_type,
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
                # NON-BLOCKING: ke alerts_non_idx.json
                self.alert_manager.log_alert(
                    source_name,
                    "symbol_not_resolved",
                    payload
                )
            except Exception:
                logger.warning(f"[alert] symbol_not_resolved for {source_name} (emiten='{emiten_name}')")

        return filing

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
