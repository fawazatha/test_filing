from __future__ import annotations
from typing import Optional, Dict, List, Any
from pathlib import Path
import os, logging, json

log = logging.getLogger("generator.filings.company_info")


class CompanyInfoProvider:
    """Interface: return {'company_name','sector','sub_sector'} or None."""
    def get_company_info(self, symbol: str) -> Optional[Dict[str, str]]:
        raise NotImplementedError


class NullCompanyInfoProvider(CompanyInfoProvider):
    def get_company_info(self, symbol: str) -> Optional[Dict[str, str]]:
        return None


# ---------- Supabase generic table provider ----------
class _SupabaseTableProvider(CompanyInfoProvider):
    def __init__(self, table_name: str) -> None:
        from supabase import create_client  # type: ignore
        url = os.getenv("SUPABASE_URL")
        key = (
            os.getenv("SUPABASE_KEY")
            or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
            or os.getenv("SUPABASE_ANON_KEY")
        )
        if not (url and key):
            raise RuntimeError("Supabase env not configured")
        self.client = create_client(url, key)
        self.table = table_name
        self._cache: Dict[str, Optional[Dict[str, str]]] = {}

    def _select_once(self, sym: str) -> Optional[Dict[str, str]]:
        resp = self.client.table(self.table) \
            .select("symbol,company_name,sector,sub_sector") \
            .eq("symbol", sym) \
            .limit(1) \
            .execute()
        if getattr(resp, "data", None):
            row = resp.data[0]
            return {
                "company_name": row.get("company_name") or "",
                "sector": row.get("sector") or "",
                "sub_sector": row.get("sub_sector") or "",
            }
        return None

    def _fetch_variants(self, symbol: str) -> Optional[Dict[str, str]]:
        if not symbol:
            return None
        s = symbol.strip()
        cands = [s, s.upper()]
        if not s.upper().endswith(".JK"):
            cands += [f"{s}.JK", f"{s.upper()}.JK"]
        base = s.replace(".JK", "")
        if base:
            cands += [base, base.upper()]

        seen = set()
        for cand in cands:
            if cand in seen:
                continue
            seen.add(cand)
            try:
                info = self._select_once(cand)
            except Exception as e:
                log.debug("Supabase %s error(%s): %s", self.table, cand, e)
                info = None
            if info:
                return info
        return None

    def get_company_info(self, symbol: str) -> Optional[Dict[str, str]]:
        if symbol in self._cache:
            return self._cache[symbol]
        info = self._fetch_variants(symbol)
        self._cache[symbol] = info
        return info


class SupabaseProfileProvider(_SupabaseTableProvider):
    def __init__(self) -> None:
        super().__init__(table_name=os.getenv("COMPANY_PROFILE_TABLE", "idx_company_profile"))


class SupabaseReportProvider(_SupabaseTableProvider):
    def __init__(self) -> None:
        super().__init__(table_name=os.getenv("COMPANY_REPORT_TABLE", "idx_company_report"))


# ---------- Local cache provider ----------
class LocalCompanyInfoProvider(CompanyInfoProvider):
    """Read from local JSON; tolerant of several shapes."""
    def __init__(self, path: Path) -> None:
        self.index: Dict[str, Dict[str, str]] = {}
        if not path.exists():
            log.debug("Local company map not found at %s", path)
            return
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            log.debug("Failed reading company map %s: %s", path, e)
            return

        def _row_to_info(row: Dict[str, Any]) -> Dict[str, str]:
            return {
                "company_name": row.get("company_name") or row.get("name") or row.get("company") or "",
                "sector": row.get("sector") or row.get("industry") or row.get("sector_name") or "",
                "sub_sector": row.get("sub_sector") or row.get("subsector") or row.get("sub_industry") or row.get("sub_sector_name") or "",
            }

        def _store(sym: str, info: Dict[str, str]) -> None:
            if not sym:
                return
            s = sym.strip()
            su = s.upper()
            self.index[s] = info
            self.index[su] = info
            self.index[su.replace(".JK", "")] = info
            if not su.endswith(".JK"):
                self.index[su + ".JK"] = info

        def _ingest_one(obj: Any, sym_hint: Optional[str] = None) -> None:
            if isinstance(obj, dict):
                sym = str(obj.get("symbol") or obj.get("ticker") or obj.get("code") or sym_hint or "").strip()
                if not sym:
                    return
                _store(sym, _row_to_info(obj))
            elif isinstance(obj, str):
                _store(obj, {"company_name": "", "sector": "", "sub_sector": ""})

        if isinstance(raw, list):
            for row in raw:
                _ingest_one(row)
        elif isinstance(raw, dict):
            if "companies" in raw and isinstance(raw["companies"], list):
                for row in raw["companies"]:
                    _ingest_one(row)
            else:
                for key, row in raw.items():
                    if isinstance(row, (dict, str)):
                        _ingest_one(row, sym_hint=str(key))

    def get_company_info(self, symbol: str) -> Optional[Dict[str, str]]:
        if not symbol:
            return None
        s = symbol.strip()
        return (
            self.index.get(s)
            or self.index.get(s.upper())
            or self.index.get(s.upper().replace(".JK", ""))
            or (self.index.get(f"{s.upper()}.JK") if not s.upper().endswith(".JK") else None)
        )


# ---------- Combined chain ----------
class CombinedProvider(CompanyInfoProvider):
    """Try providers in order; also merge fields if some are missing."""
    def __init__(self, providers: List[CompanyInfoProvider]) -> None:
        self.providers = providers

    def get_company_info(self, symbol: str) -> Optional[Dict[str, str]]:
        best: Optional[Dict[str, str]] = None
        for p in self.providers:
            info = p.get_company_info(symbol)
            if not info:
                continue
            if best is None:
                best = dict(info)
            else:
                # merge only empty fields
                if not best.get("company_name") and info.get("company_name"):
                    best["company_name"] = info["company_name"]
                if not best.get("sector") and info.get("sector"):
                    best["sector"] = info["sector"]
                if not best.get("sub_sector") and info.get("sub_sector"):
                    best["sub_sector"] = info["sub_sector"]
            # short-circuit if complete
            if best.get("sector") and best.get("sub_sector"):
                break
        return best


def build_provider(company_map_path: Optional[str] = None) -> CompanyInfoProvider:
    """
    Priority:
      1) Supabase idx_company_profile
      2) Supabase idx_company_report
      3) Local JSON cache
      4) Null
    """
    providers: List[CompanyInfoProvider] = []
    # Supabase (profile first)
    try:
        providers.append(SupabaseProfileProvider())
    except Exception:
        pass
    try:
        providers.append(SupabaseReportProvider())
    except Exception:
        pass

    # Local cache
    path = Path(company_map_path) if company_map_path else Path("data/company/company_map.json")
    providers.append(LocalCompanyInfoProvider(path))

    if not providers:
        return NullCompanyInfoProvider()
    return providers[0] if len(providers) == 1 else CombinedProvider(providers)


def get_default_provider() -> CompanyInfoProvider:
    return build_provider()
