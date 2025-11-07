from __future__ import annotations

import re
from typing import List, Optional
from rapidfuzz import fuzz

from src.common.strings import COMMON_UPPER

class NameCleaner:
    """Utilities for cleaning, validating, and matching holder names."""

    # Keep these tokens uppercased exactly as written during capitalization.
    SPECIAL_CAPS = set(COMMON_UPPER) | {"Tbk", "Ltd", "Tbk.", "Corp"}

    # Heuristic organization tokens to classify 'institution' vs 'insider'
    ORG_TOKENS = {
        "PT", "TBK", "PTE", "LTD", "LIMITED", "INC", "CORP", "CORPORATION",
        "NV", "BV", "B.V.", "GMBH", "LLC", "LP", "LLP", "PLC",
        "SDN BHD", "BHD", "BERHAD",
        "BANK", "SECURITIES", "SEKURITAS",
        "ASSET MANAGEMENT", "MANAJER INVESTASI", "INVESTMENT", "FUND",
        "YAYASAN", "FOUNDATION", "KOPERASI", "UNIVERSITAS", "PERSERO",
    }

    @classmethod
    def clean_holder_name(cls, name: str, holder_type: str) -> str:
        """
        Normalize holder name into readable title case while preserving known acronyms.
        - Removes non-printable chars, normalizes whitespace
        - Uppercases tokens in SPECIAL_CAPS, title-cases the rest
        """
        if not name:
            return ""
        s = "".join(ch for ch in str(name) if ch.isprintable())
        words = s.replace("\n", " ").strip().split()
        cleaned: List[str] = []
        for w in words:
            u = w.upper().strip(".,")
            if u in cls.SPECIAL_CAPS:
                cleaned.append(u)
            else:
                cleaned.append(w[:1].upper() + w[1:].lower())
        return " ".join(cleaned)

    @staticmethod
    def is_valid_holder(name: Optional[str]) -> bool:
        """
        Quick sanity check: reject empty, too short, or mostly-numeric names.
        Returns True if looks like a real name/organization.
        """
        if not name:
            return False
        n = name.strip()
        if len(n) < 3:
            return False
        letters = sum(1 for c in n if c.isalpha())
        ratio = letters / max(1, len(n))
        return ratio >= 0.40

    @classmethod
    def match_holder_name_to_company(
        cls,
        holder_name: str,
        company_list: List[str],
        threshold: int = 85
    ) -> str:
        """
        Fuzzy match a holder name against known company names.
        Returns the best match above threshold, or "" if none.
        """
        holder_clean = holder_name.lower().replace(".", "").strip()
        best_match = ""
        best_score = 0

        for company in company_list:
            company_clean = company.lower().replace(".", "").strip()
            score = fuzz.token_set_ratio(holder_clean, company_clean)
            if score > best_score and score >= threshold:
                best_match = company
                best_score = score

        return best_match

    @classmethod
    def classify_holder_type(cls, name: str) -> str:
        """
        Classify a holder as 'institution' if org-like tokens appear; otherwise 'insider'.
        This is heuristic and language-agnostic (ID/EN).
        """
        if not name:
            return "insider"

        name_upper = re.sub(r"\s+", " ", name).strip().upper()

        # Strong org tokens
        for token in cls.ORG_TOKENS:
            if token in name_upper:
                return "institution"

        # Common prefixes/keywords
        if re.search(r"\b(PT|CV|UD|YAYASAN|KOPERASI|BANK|SEKURITAS)\b", name_upper):
            return "institution"

        # Lightweight fallback
        name_lower = name.lower()
        if "pt" in name_lower or "tbk" in name_lower:
            return "institution"

        return "insider"

    @classmethod
    def to_title_case_custom(cls, s: str) -> str:
        """Title-case every token naïvely (used as a generic fallback)."""
        return " ".join(w[:1].upper() + w[1:].lower() for w in s.split())
