from __future__ import annotations
import re
from typing import List, Optional

from src.common.log import get_logger

logger = get_logger(__name__)

class TextExtractor:
    """
    Utility class for extracting structured values from semi-structured text.
    Refactored to improve keyword matching logic for 'purpose'.
    """

    def __init__(self, text: str):
        self.lines: List[str] = [line.strip() for line in (text or "").splitlines() if line.strip()]
        self.text: str = text or ""

    def _is_skip_line(self, line: str) -> bool:
        """Lines that look like labels/headers rather than values."""
        skip_keywords = [':', 'nama', 'kode', 'jumlah', 'persentase', 'jenis', 'tanggal']
        lo = (line or "").lower()
        # Avoid skipping if it's the *only* thing on the line (e.g., "Address:")
        if lo.endswith(':'):
             return False
        return any(tok in lo for tok in skip_keywords)

    def find_table_value(self, keyword: str) -> str:
        """
        Find a value in a "row-like" line (table-ish) that contains the keyword.
        Heuristics: try fixed-width split, regex after keyword, then next-line fallback.
        """
        if not keyword:
            return ""
        kw_lo = keyword.lower()

        for i, line in enumerate(self.lines):
            line_lo = line.lower()
            # Must match at the *start* of the line, ignoring whitespace
            if line_lo.lstrip().startswith(kw_lo):
                # 1) Try splitting by large whitespace or tabs
                parts = re.split(r"\s{3,}|\t+", line.strip())
                if len(parts) >= 2:
                    value = parts[-1].strip()
                    if value:
                        return value

                # 2) Regex: <keyword> <value>
                m = re.search(rf"{re.escape(keyword)}\s+(.+)", line, re.IGNORECASE)
                if m:
                    value = (m.group(1) or "").strip()
                    if value:
                        return value

                # 3) Fallback: look at the next 1–2 lines
                for j in range(i + 1, min(i + 3, len(self.lines))):
                    cand = self.lines[j]
                    if cand and not self._is_skip_line(cand):
                        return cand.strip()
        return ""

    def find_value_after_keyword(self, keyword: str) -> str:
        """Return the first non-empty, non-skippable line within the next 1–2 lines after a keyword line."""
        if not keyword:
            return ""
            
        kw_lo = keyword.lower()

        for i, line in enumerate(self.lines):
            line_lo = line.lower()
            if line_lo.lstrip().startswith(kw_lo):
                for j in range(i + 1, min(i + 3, len(self.lines))):
                    cand = self.lines[j]
                    if cand and not self._is_skip_line(cand):
                        return cand.strip()
        return ""

    def find_value_after_exact_line(self, keyword: str) -> str:
        """If a line equals the keyword (case-insensitive), return the next line verbatim."""
        if not keyword:
            return ""
        k = keyword.strip().lower()
        for i, line in enumerate(self.lines):
            if line.strip().lower() == k and i + 1 < len(self.lines):
                return self.lines[i + 1].strip()
        return ""

    def find_value_in_line(self, keyword: str) -> str:
        """Return the substring after the keyword in the same line (split by double spaces/tabs)."""
        if not keyword:
            return ""

        kw_lo = keyword.lower()
        for line in self.lines:
            line_lo = line.lower()
            if line_lo.lstrip().startswith(kw_lo):
                parts = re.split(r"\s{2,}|\t+", line.strip(), maxsplit=1)
                if len(parts) == 2:
                    return parts[1].strip()
        return ""

    def find_number_after_keyword(self, keyword: str) -> str:
        """
        Find a numeric value located after a keyword:
        - first try same line with 'keyword: <num>'
        - then fallback to numbers in the next 1–2 lines
        """
        if not keyword:
            return ""
        pat = re.compile(rf"{re.escape(keyword)}\s*:?\s*([0-9\.,]+)", re.IGNORECASE)

        for line in self.lines:
            m = pat.search(line)
            if m:
                return (m.group(1) or "").strip()

        kw_lo = keyword.lower()
        for i, line in enumerate(self.lines):
            line_lo = line.lower()
            if line_lo.lstrip().startswith(kw_lo):
                for j in range(i + 1, min(i + 3, len(self.lines))):
                    cand = self.lines[j]
                    if not cand:
                        continue
                    m2 = re.search(r"([0-9\.,]+)", cand)
                    if m2:
                        return m2.group(1)
        return ""

    def find_percentage_after_keyword(self, keyword: str) -> str:
        """
        Similar to find_number_after_keyword but tolerant to an optional '%' symbol.
        Returns the numeric string (without forcing normalization to float here).
        """
        if not keyword:
            return ""
        pat = re.compile(rf"{re.escape(keyword)}\s*:?\s*([0-9\.,]+)%?", re.IGNORECASE)

        for line in self.lines:
            m = pat.search(line)
            if m:
                return (m.group(1) or "").strip()

        kw_lo = keyword.lower()
        for i, line in enumerate(self.lines):
            line_lo = line.lower()
            if line_lo.lstrip().startswith(kw_lo):
                for j in range(i + 1, min(i + 3, len(self.lines))):
                    cand = self.lines[j]
                    if not cand:
                        continue
                    m2 = re.search(r"([0-9\.,]+)%?", cand)
                    if m2:
                        return m2.group(1)
        return ""

