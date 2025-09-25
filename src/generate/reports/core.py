# src/generate/reports/core.py
from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional
from dataclasses import dataclass, field
import json
from pathlib import Path
from datetime import datetime

from .utils.logger import get_logger
from .utils.datetimes import Window, fmt_for_ts_kind, JKT
from .utils import sb as sbapi

logger = get_logger("report.core")

# ===========================
# Models
# ===========================
@dataclass
class Filing:
    # Critical
    id: str
    symbol: str

    # Common metadata
    holder_name: Optional[str] = None
    holder_ticker: Optional[str] = None
    transaction_type: Optional[str] = None   # "buy"/"sell"/"transfer"/etc.
    transaction_date: Optional[str] = None   # "YYYY-MM-DD"
    timestamp: Optional[str] = None          # ISO datetime (string)
    source: Optional[str] = None

    # Numerics (optional to avoid crashes on mixed schemas)
    amount: Optional[float] = None
    price: Optional[float] = None
    transaction_value: Optional[float] = None
    holding_before: Optional[float] = None
    holding_after: Optional[float] = None
    share_pct_before: Optional[float] = None
    share_pct_after: Optional[float] = None
    share_pct_tx: Optional[float] = None

    # Keep raw row for downstream fallbacks
    raw: Dict[str, Any] = field(default_factory=dict)


# ===========================
# I/O helpers
# ===========================
def load_json_array(path: str | Path) -> List[Dict[str, Any]]:
    """Load a JSON file that may be:
       - a list[dict]
       - a dict with `rows` or `data` keys containing list[dict]
       Any other shape -> raise ValueError.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"JSON not found: {p}")
    payload = json.loads(p.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if isinstance(payload.get("rows"), list):
            return payload["rows"]
        if isinstance(payload.get("data"), list):
            return payload["data"]
    raise ValueError(f"Unsupported JSON structure in {p}")


def load_filings_from_json(path: str | Path) -> List[Filing]:
    rows = load_json_array(path)
    filings = rows_to_filings(rows)
    logger.info("Loaded %d filings from JSON.", len(filings))
    return filings


# ===========================
# Transform
# ===========================
def rows_to_filings(rows: Iterable[Dict[str, Any]]) -> List[Filing]:
    """
    Convert heterogeneous Supabase rows (legacy/new schemas) into Filing objects.
    - Tolerant to missing numerics (amount/holding_before/holding_after)
    - Tries multiple legacy aliases
    - Optionally derives fields from the first element of 'transactions' list
    - Skips rows only if missing critical (id or symbol)
    """
    def _first_nonempty(d: Dict[str, Any], *keys, default=None):
        for k in keys:
            if k in d and d[k] not in (None, ""):
                return d[k]
        return default

    def _to_float(x):
        try:
            if x is None or x == "":
                return None
            if isinstance(x, str):
                xs = x.replace(",", "").strip()
                if not xs:
                    return None
                return float(xs)
            return float(x)
        except Exception:
            return None

    out: List[Filing] = []
    skipped = 0

    for r in rows:
        # ---- Critical IDs
        fid = str(_first_nonempty(r, "id", "filing_id", "uuid", default="")).strip()
        symbol = _first_nonempty(r, "symbol", "ticker", default=None)
        if isinstance(symbol, str):
            symbol = symbol.strip().upper() or None

        if not fid or not symbol:
            skipped += 1
            logger.warning("Skipping row with missing critical fields (id/symbol): %s", r)
            continue

        # ---- Names / types / timestamps
        holder_name    = _first_nonempty(r, "holder_name", "holder", "beneficial_owner", "owner_name")
        holder_ticker  = _first_nonempty(r, "holder_ticker", "holder_symbol")
        tx_type        = _first_nonempty(r, "transaction_type", "type", "tx_type")
        tdate          = _first_nonempty(r, "transaction_date", "date", "filing_date")
        ts             = _first_nonempty(r, "timestamp", "created_at", "updated_at")
        source         = _first_nonempty(r, "source", "origin")

        # ---- Numerics (tolerate legacy aliases)
        price             = _to_float(_first_nonempty(r, "price", "avg_price", "average_price", "transaction_price"))
        transaction_value = _to_float(_first_nonempty(r, "transaction_value", "value", "tx_value"))
        amount            = _to_float(_first_nonempty(r, "amount", "tx_amount", "transaction_amount", "shares", "volume"))
        holding_before    = _to_float(_first_nonempty(r, "holding_before", "before_holding", "holding_before_total", "holdings_before", "holder_before"))
        holding_after     = _to_float(_first_nonempty(r, "holding_after", "after_holding", "holding_after_total", "holdings_after", "holder_after"))
        sp_before         = _to_float(_first_nonempty(r, "share_percentage_before", "share_pct_before", "pct_before", "percentage_before"))
        sp_after          = _to_float(_first_nonempty(r, "share_percentage_after", "share_pct_after", "pct_after", "percentage_after"))
        sp_tx             = _to_float(_first_nonempty(r, "share_percentage_transaction", "share_pct_tx", "pct_tx", "percentage_tx"))

        # ---- Derive from first transaction item if present
        txs = r.get("transactions")
        if isinstance(txs, list) and txs:
            first = txs[0] or {}
            if amount is None:
                amount = _to_float(_first_nonempty(first, "amount", "shares", "volume"))
            if price is None:
                price = _to_float(_first_nonempty(first, "price", "avg_price", "average_price"))
            if transaction_value is None:
                transaction_value = _to_float(_first_nonempty(first, "transaction_value", "value"))
            if holding_before is None:
                holding_before = _to_float(_first_nonempty(first, "holding_before", "before"))
            if holding_after is None:
                holding_after  = _to_float(_first_nonempty(first, "holding_after", "after"))
            if sp_before is None:
                sp_before = _to_float(_first_nonempty(first, "share_percentage_before", "pct_before"))
            if sp_after is None:
                sp_after  = _to_float(_first_nonempty(first, "share_percentage_after", "pct_after"))
            if sp_tx is None:
                sp_tx     = _to_float(_first_nonempty(first, "share_percentage_transaction", "pct_tx"))

        out.append(Filing(
            id=fid,
            symbol=symbol,
            holder_name=holder_name,
            holder_ticker=holder_ticker,
            transaction_type=tx_type,
            price=price,
            transaction_value=transaction_value,
            share_pct_tx=sp_tx,
            share_pct_after=sp_after,
            share_pct_before=sp_before,
            transaction_date=tdate,
            timestamp=ts,
            amount=amount,
            source=source or r.get("source"),
            holding_before=holding_before,
            holding_after=holding_after,
            raw=r,
        ))

    if skipped:
        logger.warning("rows_to_filings: skipped %d rows missing critical fields (id/symbol).", skipped)
    return out


# ===========================
# Company chooser (for fetch-first planning)
# ===========================
def _safe_list(v: Any) -> List[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        s = v.strip()
        # JSON array?
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
        # Comma-separated?
        if "," in s:
            return [x.strip() for x in s.split(",") if x.strip()]
        return [s]
    return [v]


def load_companies_from_json(path: str | Path) -> List[Dict[str, Any]]:
    """Load generic company rows from a JSON file (list of dicts)."""
    return load_json_array(path)


# --- sync impl + async wrapper for cli.py compatibility ---
def _fetch_company_report_symbols_sync(
    window: Optional[Window] = None,
    ts_kind: Optional[str] = None,
    company_map_json: Optional[str] = None,
    min_tag_substring: str = "insider",
) -> List[str]:
    """
    Sync implementation used by the async wrapper.
    """
    selected: List[str] = []
    if company_map_json:
        try:
            rows = load_companies_from_json(company_map_json)
            for r in rows:
                sym = str(r.get("symbol") or "").strip().upper()
                if not sym:
                    continue
                tags = _safe_list(r.get("tags"))
                tag_str = " ".join([str(t) for t in tags]).lower()
                if min_tag_substring.lower() in tag_str:
                    selected.append(sym)
            selected = sorted(sorted(set(selected)))
            logger.info("Company report: %d insider-tagged companies", len(selected))
            return selected
        except Exception as e:
            logger.warning("Failed to load company map (%s): %s", company_map_json, e)

    logger.info("Company report: no company_map_json provided — returning empty symbol list (fetch-all mode).")
    return []


async def fetch_company_report_symbols(
    window: Optional[Window] = None,
    ts_kind: Optional[str] = None,
    company_map_json: Optional[str] = None,
    min_tag_substring: str = "insider",
) -> List[str]:
    """
    Async wrapper kept for compatibility with cli.py which does:
        companies = await fetch_company_report_symbols(...)
    """
    return _fetch_company_report_symbols_sync(
        window=window,
        ts_kind=ts_kind,
        company_map_json=company_map_json,
        min_tag_substring=min_tag_substring,
    )


# ===========================
# Supabase fetchers
# ===========================
async def fetch_filings_for_symbols_between(
    symbols: List[str],
    window: Window,
    *,
    ts_col: str = "timestamp",
    ts_kind: str = "timestamp",
    table: str = "idx_filings",
) -> List[Filing]:
    """Fetch filings from Supabase for a set of symbols within a half-open window (gt start, lt end)."""
    start_val = fmt_for_ts_kind(window.start, ts_kind)
    end_val   = fmt_for_ts_kind(window.end, ts_kind)

    filters = [(ts_col, f"gt.{start_val}"), (ts_col, f"lt.{end_val}")]
    in_filters = {"symbol": [str(s).upper() for s in symbols]} if symbols else None

    # Keep select list narrow but sufficient for rows_to_filings
    select = (
        "id,symbol,holder_name,holder_ticker,transaction_type,transaction_date,"
        "timestamp,source,price,transaction_value,amount,holding_before,holding_after,"
        "share_percentage_before,share_percentage_after,share_percentage_transaction,transactions"
    )
    rows = await sbapi.fetch(
        table=table,
        select=select,
        filters=filters,
        in_filters=in_filters,
        order=f"{ts_col}.asc,id.asc",
        page_size=1000,
        timeout=60.0,
    )
    logger.info("Supabase returned %d rows (table=%s)", len(rows), table)
    return rows_to_filings(rows)


def filter_filings_by_window(
    filings: List[Filing],
    window: Window,
    *,
    ts_col: str = "timestamp",
    ts_kind: str = "timestamp",
) -> List[Filing]:
    """
    In-memory filter for already-loaded filings (JSON input path).
    ts_col: the source column name (string inside f.raw) that holds the timestamp we compare.
    """
    def _parse_ts(s: Optional[str]) -> Optional[datetime]:
        if not s:
            return None
        try:
            # Accept both "YYYY-MM-DD HH:MM:SS" and ISO w/ tz. Assume JKT if naive.
            if "T" in s or "Z" in s or "+" in s:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            else:
                dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            if dt.tzinfo is None:
                return dt.replace(tzinfo=JKT)
            return dt
        except Exception:
            return None

    start = window.start
    end = window.end
    out: List[Filing] = []
    for f in filings:
        ts_str = None
        # prefer raw[ts_col], then f.timestamp
        if isinstance(f.raw, dict):
            ts_str = f.raw.get(ts_col) or f.timestamp
        else:
            ts_str = f.timestamp
        dt = _parse_ts(ts_str) if ts_str else None
        if dt and (dt > start) and (dt < end):  # half-open
            out.append(f)
    logger.info("filter_filings_by_window: %d -> %d within %s..%s", len(filings), len(out), start, end)
    return out


# ===========================
# Grouping for email/report
# ===========================
# ===========================
# Grouping for email/report
# ===========================
def group_report(*args) -> Dict[str, Any]:
    """
    Back-compatible adapter:
      - group_report(filings)
      - group_report(companies, filings)  # older cli.py
    Returns a dict consumable by the email/html template.
    """
    if len(args) == 1:
        filings = args[0]
    elif len(args) == 2:
        # We ignore the companies list for grouping; it’s only used upstream for planning.
        _, filings = args
    else:
        raise TypeError("group_report expects (filings) or (companies, filings)")

    by_symbol: Dict[str, Dict[str, Any]] = {}
    for f in filings:
        s = f.symbol
        if s not in by_symbol:
            by_symbol[s] = {"symbol": s, "filings": []}
        by_symbol[s]["filings"].append({
            "id": f.id,
            "symbol": f.symbol,
            "holder_name": f.holder_name,
            "holder_ticker": f.holder_ticker,
            "transaction_type": f.transaction_type,
            "price": f.price,
            "transaction_value": f.transaction_value,
            "amount": f.amount,
            "holding_before": f.holding_before,
            "holding_after": f.holding_after,
            "share_percentage_before": f.share_pct_before,
            "share_percentage_transaction": f.share_pct_tx,
            "share_percentage_after": f.share_pct_after,
            "transaction_date": f.transaction_date,
            "timestamp": f.timestamp,
            "source": f.source or f.raw.get("source"),
        })

    # sort filings per symbol by timestamp then id (stable)
    for s in by_symbol.values():
        s["filings"].sort(key=lambda x: (x.get("timestamp") or "", x["id"]))

    return {
        "total_companies": len({f.symbol for f in filings}),
        "total_filings": len(filings),
        "companies": sorted(by_symbol.values(), key=lambda x: x["symbol"]),
    }

