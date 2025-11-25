# src/workflow/channels/sheet/sender.py
from __future__ import annotations

import re

from datetime import datetime
from typing import Any, Dict, List, Optional

from src.common.log import get_logger
from src.workflow.models import Workflow, WorkflowEvent
from .formatter import build_sheet_blocks
from src.services.sheet.google import append_rows

logger = get_logger("workflow.sheet.sender")


def _extract_spreadsheet_id(raw: str) -> str:
    """
    Terima:
      - '1Use3BVA.....'  (ID langsung)
      - 'https://docs.google.com/spreadsheets/d/1Use3BVA.../edit#gid=0'

    Return:
      - '1Use3BVA.....'
    """
    raw = (raw or "").strip()
    if raw.startswith("http://") or raw.startswith("https://"):
        m = re.search(r"/d/([a-zA-Z0-9-_]+)", raw)
        if m:
            return m.group(1)
    return raw

def _resolve_sheet_cfg(workflow: Workflow) -> Dict[str, Optional[str]]:
    """
    Resolve sheet channel configuration.

    Expected shapes (after _normalize_channels):
      - {"sheet": {"id": "SPREADSHEET_ID_OR_URL"}}
      - {"sheet": {"value": "SPREADSHEET_ID_OR_URL"}}
      - {"sheet": "SPREADSHEET_ID_OR_URL"}
    """
    channels = getattr(workflow, "channels", None) or {}
    raw = channels.get("sheet")

    # Raw string: treat it directly as spreadsheet id or URL
    if isinstance(raw, str):
        return {"spreadsheet_id": _extract_spreadsheet_id(raw)}

    if not isinstance(raw, dict):
        return {"spreadsheet_id": None}

    val = raw.get("id") or raw.get("spreadsheet_id") or raw.get("value")
    if isinstance(val, str):
        return {"spreadsheet_id": _extract_spreadsheet_id(val)}

    return {"spreadsheet_id": None}


async def send_sheet_for_workflow(
    *,
    workflow: Workflow,
    events: List[WorkflowEvent],
    window_start_label: str,
    window_end_label: str,
) -> None:
    """
    Append rows to Google Sheet for this workflow.

    - Each tag is mapped to a specific sheet/tab (see SHEET_CONFIG in formatter.py).
    - All runs are historical: we never overwrite; we always append.
    """
    if append_rows is None:
        logger.warning(
            "Sheet service is not available (append_rows import failed); "
            "skipping sheet channel for workflow %s (%s)",
            workflow.id,
            workflow.name,
        )
        return

    if not events:
        logger.info(
            "Workflow %s (%s) has no events for Sheet, skipping",
            workflow.id,
            workflow.name,
        )
        return

    cfg = _resolve_sheet_cfg(workflow)
    spreadsheet_id = cfg["spreadsheet_id"]

    if not spreadsheet_id:
        logger.info(
            "Workflow %s (%s) has sheet channel but no spreadsheet_id configured, skipping",
            workflow.id,
            workflow.name,
        )
        return

    # Build blocks per tag: { tag: { sheet_name, headers, rows } }
    blocks = build_sheet_blocks(events, as_of=None)

    if not blocks:
        logger.info(
            "Workflow %s (%s) produced no sheet blocks (no mapped tags), skipping",
            workflow.id,
            workflow.name,
        )
        return

    # Append to each sheet/tab
    for tag, block in blocks.items():
        sheet_name = block.get("sheet_name")
        headers: List[str] = block.get("headers") or []
        rows: List[List[Any]] = block.get("rows") or []

        if not sheet_name or not rows:
            continue

        logger.info(
            "Appending %d rows to spreadsheet=%s sheet=%s for workflow %s (%s), tag=%s",
            len(rows),
            spreadsheet_id,
            sheet_name,
            workflow.id,
            workflow.name,
            tag,
        )

        try:
            append_rows(
                spreadsheet_id=spreadsheet_id,
                sheet_name=sheet_name,
                headers=headers,
                rows=rows,
            )
        except Exception as exc:  # pragma: no cover
            logger.exception(
                "Failed to append rows to spreadsheet=%s sheet=%s for workflow %s (%s): %s",
                spreadsheet_id,
                sheet_name,
                workflow.id,
                workflow.name,
                repr(exc),
            )
