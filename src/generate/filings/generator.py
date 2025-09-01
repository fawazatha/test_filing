from __future__ import annotations

from typing import List, Optional, Any, Dict, Tuple
from pathlib import Path
import logging
from datetime import datetime

# Try to use your project logger, but be tolerant about signatures.
try:
    from src.utils.logger import get_logger as _project_get_logger  # type: ignore
except Exception:
    _project_get_logger = None


def _make_logger(name: str, verbose: bool = False) -> logging.Logger:
    if _project_get_logger:
        try:
            lg = _project_get_logger(name)  # type: ignore
            if verbose:
                lg.setLevel(logging.DEBUG)
            return lg
        except TypeError:
            try:
                return _project_get_logger(name, verbose=verbose)  # type: ignore
            except Exception:
                pass
        except Exception:
            pass
    lg = logging.getLogger(name)
    if not lg.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        lg.addHandler(h)
    lg.setLevel(logging.DEBUG if verbose else logging.INFO)
    return lg

from .utils.loader import load_idx_dicts, load_non_idx_dicts
from .utils.normalizer import normalize_idx_record, normalize_non_idx_record
from .utils.validator import FilingValidation
from .utils.writer import apply_uid_scenarios  # post-pass scenarios


class FilingGenerator:
    def __init__(self, verbose: bool = False, company_lookup: Optional[Dict[str, str]] = None, uid_window: int = 3) -> None:
        self.log = _make_logger("generator.filings", verbose=verbose)
        self.validator = FilingValidation()
        self.company_lookup = dict(company_lookup or {})
        self.uid_window = uid_window

    # ---------- helpers ----------
    @staticmethod
    def _parse_ts(s: Optional[str]) -> datetime:
        """
        Parse ISO-like timestamp 'YYYY-MM-DDTHH:MM:SS' (optionally with 'Z').
        Fallback to min datetime if empty/invalid.
        """
        if not s:
            return datetime.min
        try:
            return datetime.fromisoformat(s.replace("Z", ""))
        except Exception:
            # last resort: try simple date (kept for backward compat if any)
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
                try:
                    return datetime.strptime(s, fmt)
                except Exception:
                    continue
            return datetime.min

    @staticmethod
    def _key_for_sort(f: Any) -> Tuple[str, datetime, str]:
        """
        Sort by (symbol, timestamp, raw-id/title). We prefer 'timestamp'.
        """
        if isinstance(f, dict):
            sym = (f.get("symbol") or "").upper().strip()
            ts  = f.get("timestamp") or ""
            raw = f.get("raw_id") or f.get("title") or ""
        else:
            sym = (getattr(f, "symbol", "") or "").upper().strip()
            ts  = getattr(f, "timestamp", "") or ""
            raw = getattr(f, "raw_id", None) or getattr(f, "title", "") or ""
        return (sym, FilingGenerator._parse_ts(ts), str(raw))

    @staticmethod
    def _filename_from_any(*objs: Any) -> str:
        """
        Try to extract a PDF filename from various shapes:
        - obj.get('link', {}).get('filename')
        - obj.get('source') (take last path segment)
        - obj.get('link', {}).get('url') (last segment)
        """
        for obj in objs:
            if not obj:
                continue
            if isinstance(obj, dict):
                link = obj.get("link") or {}
                if isinstance(link, dict):
                    fn = (link.get("filename") or "").strip()
                    if fn:
                        return fn
                    url = (link.get("url") or "").strip()
                    if url:
                        seg = url.split("/")[-1]
                        if seg:
                            return seg
                src = (obj.get("source") or "").strip()
                if src:
                    seg = src.split("/")[-1]
                    if seg:
                        return seg
        return ""

    @staticmethod
    def _attach_timestamp_inplace(filing: Dict[str, Any], downloads_map: Dict[str, Dict[str, Any]], *parsed_contexts: Any) -> None:
        """
        Set filing['timestamp'] from downloads_map[filename]['timestamp'] if available.
        Also mirror into filing['link']['timestamp'] if a link dict exists.
        Remove legacy date fields ('transaction_date'/'date').
        """
        filename = FilingGenerator._filename_from_any(filing, *parsed_contexts)
        dl = downloads_map.get(filename or "", {})
        ts = (dl.get("timestamp") or "").strip()
        if ts:
            filing["timestamp"] = ts
            link = filing.get("link")
            if isinstance(link, dict):
                link["timestamp"] = ts

        # Remove legacy fields to comply with "ganti date jadi timestamp"
        filing.pop("transaction_date", None)
        filing.pop("date", None)

    # ---------- main ----------
    def run(
        self,
        idx_path: Optional[Path],
        non_idx_path: Optional[Path],
        downloads_map: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        filings: List[Dict[str, Any]] = []
        downloads_map = downloads_map or {}

        # --- IDX ---
        if idx_path and idx_path.exists():
            self.log.info("Loading IDX parsed records from %s", idx_path)
            for rec in load_idx_dicts(idx_path):
                filing = normalize_idx_record(rec)  # expected to compute direction/netting
                # Attach timestamp from downloaded_pdfs.json (by filename)
                self._attach_timestamp_inplace(filing, downloads_map, rec)

                warn = self.validator.validate(filing)
                if warn:
                    sym = filing.get("symbol") if isinstance(filing, dict) else getattr(filing, "symbol", "?")
                    self.log.warning("[IDX][%s] %s", sym or "?", "; ".join(warn))
                filings.append(filing)

        # --- NON-IDX ---
        if non_idx_path and non_idx_path.exists():
            self.log.info("Loading NON-IDX parsed records from %s", non_idx_path)
            for rec in load_non_idx_dicts(non_idx_path):
                # Preferred: normalizer supports downloads_map for ticker->symbol injection
                try:
                    filing = normalize_non_idx_record(rec, downloads_map=downloads_map)  # type: ignore[call-arg]
                except TypeError:
                    # Fallback: inject symbol from downloads_map using filename
                    f = dict(rec) if isinstance(rec, dict) else rec
                    filename = self._filename_from_any(f)
                    dl = downloads_map.get(filename or "", {})
                    ticker = (dl.get("ticker") or "").strip().upper()
                    if isinstance(f, dict) and ticker and not f.get("symbol"):
                        f["symbol"] = ticker
                    filing = normalize_non_idx_record(f)  # type: ignore[misc]

                # Attach timestamp from downloaded_pdfs.json (by filename)
                self._attach_timestamp_inplace(filing, downloads_map, rec)

                warn = self.validator.validate(filing)
                if warn:
                    sym = filing.get("symbol") if isinstance(filing, dict) else getattr(filing, "symbol", "?")
                    self.log.warning("[NON-IDX][%s] %s", sym or "?", "; ".join(warn))
                filings.append(filing)

        # Sort so that ±window pairing is meaningful (by symbol + timestamp)
        filings.sort(key=self._key_for_sort)

        # Post-pass UID scenarios (Scenario 1/2/3)
        apply_uid_scenarios(filings, company_lookup=self.company_lookup, uid_window=self.uid_window)

        self.log.info("Generated %d filings", len(filings))
        return filings
