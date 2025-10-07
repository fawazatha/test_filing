import re
from typing import List
from rapidfuzz import fuzz

class NameCleaner:
    """Utility class for cleaning and matching names."""
    
    SPECIAL_CAPS = {"PT", "TBK", "LTD", "LIMITED", "Tbk", "Ltd", "Tbk.", "Corp"}
    ORG_TOKENS = {
        "PT", "TBK", "PTE", "LTD", "LIMITED", "INC", "CORP", "CORPORATION",
        "NV", "BV", "B.V.", "GMBH", "LLC", "LP", "LLP", "PLC",
        "SDN BHD", "BHD", "BERHAD",
        "BANK", "SECURITIES", "SEKURITAS",
        "ASSET MANAGEMENT", "MANAJER INVESTASI", "INVESTMENT", "FUND",
        "YAYASAN", "FOUNDATION", "KOPERASI", "UNIVERSITAS", "PERSERO"
    }
    
    @classmethod
    def clean_holder_name(cls, name: str, holder_type: str) -> str:
        """Clean holder name with proper capitalization."""
        if not name:
            return ""
        # buang karakter tak terlihat + normalisasi spasi
        name = "".join(ch for ch in str(name) if ch.isprintable())
        words = name.replace("\n", " ").strip().split()
        cleaned_words = []
        for word in words:
            upper_word = word.upper().strip(".,")
            if upper_word in cls.SPECIAL_CAPS:
                cleaned_words.append(upper_word)
            else:
                cleaned_words.append(word[:1].upper() + word[1:].lower())
        return " ".join(cleaned_words)


    @staticmethod
    def is_valid_holder(name: str | None) -> bool:
        """Reject empty, too short, or mostly-numeric holder names."""
        if not name:
            return False
        n = name.strip()
        if len(n) < 3:
            return False
        letters = sum(1 for c in n if c.isalpha())
        ratio = letters / max(1, len(n))
        return ratio >= 0.40
    
    @classmethod
    def match_holder_name_to_company(cls, holder_name: str, company_list: List[str], threshold: int = 85) -> str:
        """Match holder name to company using fuzz matching."""
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
        """Classify holder type based on name."""
        if not name:
            return "insider"
        
        name_upper = re.sub(r"\s+", " ", name).strip().upper()
        
        # Check for organization tokens
        for token in cls.ORG_TOKENS:
            if token in name_upper:
                return "institution"
        
        # Check for common prefixes
        if re.search(r"\b(PT|CV|UD|YAYASAN|KOPERASI|BANK|SEKURITAS)\b", name_upper):
            return "institution"
        
        name_lower = name.lower()
        if "pt" in name_lower or "tbk" in name_lower:
            return "institution"
        
        return "insider"
    
    @classmethod
    def to_title_case_custom(cls, s: str) -> str:
        """Convert string to custom title case."""
        return " ".join(word[:1].upper() + word[1:].lower() for word in s.split())

