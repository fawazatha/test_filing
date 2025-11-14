"""
alerts_to_sheet.py

Small helper to:
- load workflows from JSON
- take one idx_company_report row (dict)
- generate sheet rows for matching workflows + tags
- append to Google Sheets using gspread

Currently supports tags:
- 90-d-high
- public-float-under-25
- upcoming-dividend

You can extend TAG_HANDLERS for more tags later.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import gspread
from google.oauth2.service_account import Credentials


# =========================
# Google Sheets client
# =========================

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SHEET_COLUMNS = [
    "created_at",
    "workflow_id",
    "workflow_name",
    "user_id",
    "symbol",
    "company_name",
    "sector",
    "sub_sector",
    "source",
    "tag",
    "as_of_date",
    "indicator_column",
    "indicator_value",
    "extra_json",
]


def get_sheet(sheet_id: str, service_account_file: str):
    """Return the first worksheet of a Google Sheet."""
    creds = Credentials.from_service_account_file(service_account_file, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(sheet_id).sheet1


def ensure_header(sheet) -> None:
    """Ensure the header row exists on row 1."""
    existing = sheet.row_values(1)
    if not existing:
        sheet.insert_row(SHEET_COLUMNS, 1)
    elif existing != SHEET_COLUMNS:
        # You might want to raise or log a warning here instead
        print("[WARN] Sheet header does not match expected columns.")


# =========================
# Data models
# =========================

@dataclass
class Workflow:
    id: int
    user_id: Optional[int]
    name: str
    exchange: str
    tickers: List[str]
    tags: List[str]
    channels: List[Dict[str, Any]]

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Workflow":
        channels = d.get("channels", [])
        # Backward compatibility: channels as dict instead of list
        if isinstance(channels, dict):
            channels = [channels]
        return cls(
            id=d["id"],
            user_id=d.get("user_id"),
            name=d["name"],
            exchange=d.get("exchange", "IDX"),
            tickers=d.get("tickers", []),
            tags=d.get("tags", []),
            channels=channels,
        )


# =========================
# Helpers for idx_company_report JSON fields
# =========================

def parse_json_field(row: Dict[str, Any], key: str) -> Any:
    """Parse a JSON-encoded string field if necessary."""
    value = row.get(key)
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return value


def get_public_float_pct(row: Dict[str, Any]) -> Optional[float]:
    """Extract 'Public' shareholder percentage from major_shareholders field."""
    mh_list = parse_json_field(row, "major_shareholders") or []
    for holder in mh_list:
        if holder.get("name") == "Public":
            pct_str = holder.get("share_percentage")
            if pct_str is None:
                return None
            try:
                # In sample, share_percentage is '0.1881' meaning 18.81%
                value = float(pct_str) * 100.0
            except ValueError:
                return None
            return value
    return None


# =========================
# Tag-specific row builders
# =========================

def build_rows_for_90d_high(
    row: Dict[str, Any],
    base: Dict[str, Any],
) -> List[List[Any]]:
    """Build sheet rows for tag '90-d-high'."""
    all_time = parse_json_field(row, "all_time_price") or {}
    high90 = all_time.get("90_d_high") or {}
    price = high90.get("price")
    date = high90.get("date") or row.get("latest_close_date")

    rows: List[List[Any]] = []
    if price is not None:
        r = base.copy()
        r["tag"] = "90-d-high"
        r["as_of_date"] = date
        r["indicator_column"] = "90d_high_price"
        r["indicator_value"] = price
        rows.append(to_row_list(r))

    # If you want a second row for the date itself, uncomment below:
    # if date:
    #     r2 = base.copy()
    #     r2["tag"] = "90-d-high"
    #     r2["as_of_date"] = date
    #     r2["indicator_column"] = "90d_high_date"
    #     r2["indicator_value"] = date
    #     rows.append(to_row_list(r2))

    return rows


def build_rows_for_public_float_under_25(
    row: Dict[str, Any],
    base: Dict[str, Any],
) -> List[List[Any]]:
    """Build sheet rows for tag 'public-float-under-25'."""
    pct = get_public_float_pct(row)
    if pct is None:
        return []

    r = base.copy()
    r["tag"] = "public-float-under-25"
    # Use latest_close_date as of_date for ownership-related tags
    r["as_of_date"] = row.get("latest_close_date")
    r["indicator_column"] = "public_float_pct"
    r["indicator_value"] = pct
    return [to_row_list(r)]


def build_rows_for_upcoming_dividend(
    row: Dict[str, Any],
    base: Dict[str, Any],
) -> List[List[Any]]:
    """Build sheet rows for tag 'upcoming-dividend'."""
    upcoming = parse_json_field(row, "upcoming_dividends") or []
    if not upcoming:
        return []

    # For now, only take the first upcoming dividend
    dv = upcoming[0]
    dividend_amount = dv.get("dividend_amount")
    ex_date = dv.get("ex_date")
    payment_date = dv.get("payment_date")

    rows: List[List[Any]] = []

    if dividend_amount is not None:
        r = base.copy()
        r["tag"] = "upcoming-dividend"
        r["as_of_date"] = ex_date or row.get("latest_close_date")
        r["indicator_column"] = "dividend_amount"
        r["indicator_value"] = dividend_amount
        rows.append(to_row_list(r))

    if ex_date:
        r2 = base.copy()
        r2["tag"] = "upcoming-dividend"
        r2["as_of_date"] = ex_date
        r2["indicator_column"] = "ex_date"
        r2["indicator_value"] = ex_date
        rows.append(to_row_list(r2))

    if payment_date:
        r3 = base.copy()
        r3["tag"] = "upcoming-dividend"
        r3["as_of_date"] = ex_date or payment_date
        r3["indicator_column"] = "payment_date"
        r3["indicator_value"] = payment_date
        rows.append(to_row_list(r3))

    return rows


# Registry of tag -> handler
TAG_HANDLERS = {
    "90-d-high": build_rows_for_90d_high,
    "public-float-under-25": build_rows_for_public_float_under_25,
    "upcoming-dividend": build_rows_for_upcoming_dividend,
}


# =========================
# Core row builder
# =========================

def to_row_list(d: Dict[str, Any]) -> List[Any]:
    """Convert dict with our keys to a list in SHEET_COLUMNS order."""
    return [d.get(col) for col in SHEET_COLUMNS]


def build_base_row_dict(
    workflow: Workflow,
    row: Dict[str, Any],
    source: str = "company_report",
) -> Dict[str, Any]:
    """Build the common part of a sheet row that is shared among tags."""
    created_at = datetime.now(timezone.utc).isoformat()

    base = {
        "created_at": created_at,
        "workflow_id": workflow.id,
        "workflow_name": workflow.name,
        "user_id": workflow.user_id,
        "symbol": row.get("symbol"),
        "company_name": row.get("company_name"),
        "sector": row.get("sector"),
        "sub_sector": row.get("sub_sector"),
        "source": source,
        "tag": None,
        "as_of_date": None,
        "indicator_column": None,
        "indicator_value": None,
        "extra_json": None,
    }
    return base


def build_sheet_rows_for_company_row(
    workflow: Workflow,
    company_row: Dict[str, Any],
) -> List[List[Any]]:
    """
    For a given workflow and a single idx_company_report row,
    build all sheet rows for intersecting tags.
    """
    symbol = company_row.get("symbol")
    if symbol not in workflow.tickers:
        return []

    row_tags: List[str] = company_row.get("tags", [])
    base = build_base_row_dict(workflow, company_row)

    all_rows: List[List[Any]] = []
    for tag in row_tags:
        if tag not in workflow.tags:
            continue
        handler = TAG_HANDLERS.get(tag)
        if not handler:
            continue
        tag_rows = handler(company_row, base)
        all_rows.extend(tag_rows)

    return all_rows


# =========================
# Public API
# =========================

def load_workflows(path: str) -> List[Workflow]:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return [Workflow.from_dict(d) for d in raw]


def append_alerts_for_company_row(
    company_row: Dict[str, Any],
    workflows: List[Workflow],
    service_account_file: str,
) -> None:
    """
    For a single idx_company_report row, generate alerts
    for all workflows and append them to the corresponding Sheets.

    Each workflow may have one or more sheet channels in workflow.channels.
    """
    # Collect rows per sheet_id
    rows_by_sheet: Dict[str, List[List[Any]]] = {}

    for wf in workflows:
        if wf.exchange != "IDX":
            continue

        wf_rows = build_sheet_rows_for_company_row(wf, company_row)
        if not wf_rows:
            continue

        for ch in wf.channels:
            sheet_id = ch.get("sheet")
            if not sheet_id:
                continue
            rows_by_sheet.setdefault(sheet_id, []).extend(wf_rows)

    # Append to each sheet
    for sheet_id, rows in rows_by_sheet.items():
        print(f"[INFO] Appending {len(rows)} rows to sheet {sheet_id}")
        sheet = get_sheet(sheet_id, service_account_file)
        ensure_header(sheet)
        sheet.append_rows(rows, value_input_option="USER_ENTERED")


# =========================
# Example usage (manual test)
# =========================

if __name__ == "__main__":
    # Example: load one idx_company_report row from JSON file
    # and send alerts for all workflows.
    import pathlib

    base_dir = pathlib.Path(__file__).resolve().parent
    workflows_path = base_dir / "workflows.json"
    company_row_path = base_dir / "sample_idx_company_report_row.json"
    service_account_file = str(base_dir / "service_account.json")

    workflows = load_workflows(str(workflows_path))

    with open(company_row_path, "r", encoding="utf-8") as f:
        company_row = json.load(f)

    append_alerts_for_company_row(
        company_row=company_row,
        workflows=workflows,
        service_account_file=service_account_file,
    )
