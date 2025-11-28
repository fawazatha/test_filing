# src/services/sheet/google.py
from __future__ import annotations

from typing import Any, List
import json
import os

import gspread
from google.oauth2.service_account import Credentials

# Minimal scope for reading/writing Google Sheets
_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _get_client() -> gspread.Client:
    """
    Obtain a Google Sheets client from:
    - GOOGLE_SERVICE_ACCOUNT_FILE (path to the JSON file), or
    - GOOGLE_SERVICE_ACCOUNT_JSON (raw JSON content)
    """
    keyfile_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    keyfile_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

    if keyfile_json:
        info = json.loads(keyfile_json)
        creds = Credentials.from_service_account_info(info, scopes=_SCOPES)
    elif keyfile_path:
        creds = Credentials.from_service_account_file(keyfile_path, scopes=_SCOPES)
    else:
        raise RuntimeError(
            "Google Sheets credentials not configured. "
            "Set GOOGLE_SERVICE_ACCOUNT_FILE or GOOGLE_SERVICE_ACCOUNT_JSON."
        )

    return gspread.authorize(creds)


def append_rows(
    *,
    spreadsheet_id: str,
    sheet_name: str,
    headers: List[str],
    rows: List[List[Any]],
) -> None:
    """
    Append rows to a sheet (tab) in a spreadsheet.

    - If the sheet does not exist → create it.
    - If the header row is empty and headers are provided → write headers to row 1.
    - `rows` are appended below existing data.
    """
    if not rows:
        return

    client = _get_client()
    sh = client.open_by_key(spreadsheet_id)

    # Ensure the worksheet exists
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        # Create a new worksheet
        max_cols = max(len(headers), max((len(r) for r in rows), default=0))
        ws = sh.add_worksheet(title=sheet_name, rows="1000", cols=str(max_cols))

        if headers:
            ws.insert_row(headers, index=1)
    else:
        # If it exists but row 1 is empty and headers are provided → insert headers
        if headers:
            existing_header = ws.row_values(1)
            if not existing_header:
                ws.insert_row(headers, index=1)

    # Append rows at the bottom
    ws.append_rows(rows, value_input_option="USER_ENTERED")
