import os
import json
import re
import logging
from pathlib import Path
from typing import Optional, List, Dict
from supabase import create_client
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

COMPANY_MAP_PATH = Path("data/company/company_map.json")

_CORP_STOPWORDS = {"PT", "P.T", "PERSEROAN", "TERBATAS", "TBK", "TBK.", "TBK,", "TBK)", "(TBK"}
_SPLIT_RE = re.compile(r"[^A-Za-z0-9]+", re.UNICODE)

def _normalize_name(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    
    tokens = [t for t in _SPLIT_RE.split(s.lower()) if t]
    tokens = [t for t in tokens if t.upper() not in _CORP_STOPWORDS]
    return " ".join(tokens).strip()

class DatabaseHelper:
    """Helper class for database operations with local cache + Supabase fallback."""
    
    def __init__(self):
        load_dotenv()
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("Missing Supabase credentials in environment variables")
        self.supabase = create_client(self.supabase_url, self.supabase_key)

        self._symbol_to_name: Optional[Dict[str, str]] = None
        self._name_to_symbol: Optional[Dict[str, str]] = None
        self._load_local_company_map()


    def _load_local_company_map(self):
        """Read data/company/company_map.json (symbol -> company_name)."""
        try:
            if COMPANY_MAP_PATH.exists():
                data = json.loads(COMPANY_MAP_PATH.read_text(encoding="utf-8"))

                self._symbol_to_name = { (k or "").upper(): (v or "") for k, v in data.items() if k and v }
                self._name_to_symbol = {}
                for sym, raw_name in self._symbol_to_name.items():
                    key = _normalize_name(raw_name)
                    if key and key not in self._name_to_symbol:
                        self._name_to_symbol[key] = sym
            else:
                self._symbol_to_name, self._name_to_symbol = {}, {}
        except Exception as e:
            logger.warning(f"Failed to load local company map: {e}")
            self._symbol_to_name, self._name_to_symbol = {}, {}


    def get_company_name_by_symbol(self, symbol: str) -> Optional[str]:
        """
        Return company name for a given ticker.
        Tries local cache first, then Supabase.
        """
        if not symbol:
            return None
        sym = symbol.strip().upper()

        if self._symbol_to_name and sym in self._symbol_to_name:
            return self._symbol_to_name[sym]

        try:
            resp = (
                self.supabase.table("idx_company_profile")
                .select("company_name")
                .eq("symbol", sym)
                .limit(1)
                .execute()
            )
            if resp.data:
                name = (resp.data[0].get("company_name") or "").strip()
                if name:
  
                    self._symbol_to_name[sym] = name
                    key = _normalize_name(name)
                    if key and key not in self._name_to_symbol:
                        self._name_to_symbol[key] = sym
                    return name
        except Exception as e:
            logger.error(f"Error fetching company_name for symbol {sym}: {e}")

        return None

    def get_symbol_by_company_name(self, name: str) -> Optional[str]:
        """
        Return ticker for a given company name.
        Uses: normalized local reverse map -> Supabase wildcard queries (several patterns).
        """
        if not name:
            return None
        
        key = _normalize_name(name)
        if self._name_to_symbol and key in self._name_to_symbol:
            return self._name_to_symbol[key]
        
        raw = name.strip()
        patterns = [
            raw,               
            f"{raw}%",        
            f"%{raw}",        
            f"%{raw}%",        
        ]


        if key and key != _normalize_name(raw):
            raw_norm = " ".join([t for t in _SPLIT_RE.split(raw) if t and t.upper() not in _CORP_STOPWORDS]).strip()
            if raw_norm and raw_norm != raw:
                patterns += [raw_norm, f"{raw_norm}%", f"%{raw_norm}", f"%{raw_norm}%"]

        tried = set()
        for pat in patterns:
            if not pat or pat in tried:
                continue
            tried.add(pat)
            try:
                resp = (
                    self.supabase.table("idx_company_profile")
                    .select("symbol, company_name")
                    .ilike("company_name", pat) 
                    .limit(1)
                    .execute()
                )
                if resp.data:
                    sym = (resp.data[0].get("symbol") or "").upper().strip()
                    cname = (resp.data[0].get("company_name") or "").strip()
                    if sym:
                        # update caches
                        if self._symbol_to_name is not None:
                            self._symbol_to_name[sym] = cname
                        if self._name_to_symbol is not None:
                            nkey = _normalize_name(cname)
                            if nkey and nkey not in self._name_to_symbol:
                                self._name_to_symbol[nkey] = sym
                        return sym
            except Exception as e:
                logger.error(f"Error fetching symbol for company '{name}' with pattern '{pat}': {e}")

        return None

    def load_company_names(self) -> List[str]:
        """
        Load all company names (Supabase). If you have a big table, consider paging.
        """
        try:
            response = (
                self.supabase.table("idx_company_profile")
                .select("company_name")
                .execute()
            )
            if response.data:
                return [ (entry.get("company_name") or "").strip() for entry in response.data if entry.get("company_name") ]
        except Exception as e:
            logger.error(f"Error loading company names: {e}")
        return []
