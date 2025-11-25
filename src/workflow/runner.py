# src/workflow/runner.py
from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from src.common.datetime import now_wib
from src.common.log import get_logger
from src.workflow.engine import fetch_active_workflows, generate_events
from src.workflow.models import Workflow, WorkflowEvent

# Channel senders: each channel owns its own formatting/context
from src.workflow.channels.slack.sender import send_slack_for_workflow
from src.workflow.channels.email.sender import send_email_for_workflow
from src.workflow.channels.sheet.sender import send_sheet_for_workflow

try:
    # Optional: WhatsApp channel
    from src.workflow.channels.whatsapp.sender import send_whatsapp_for_workflow
except ImportError:  # pragma: no cover
    send_whatsapp_for_workflow = None  # type: ignore[assignment]



logger = get_logger("workflow.runner")

# Load .env from the project root (where you run `python -m src.workflow.runner`)
load_dotenv()


def fmt_wib_date(dt: datetime) -> str:
    """Format a WIB datetime as a compact date label."""
    return dt.strftime("%Y-%m-%d")


def _normalize_channels(raw: Any) -> Dict[str, Any]:
    """
    Normalize the `channels` field from Supabase into a dict.

    Example:
      [
        {"slack": "https://hook..."},
        {"email": "foo@bar.com"},
        {"sheet": "SPREADSHEET_ID"}
      ]
    becomes:
      {
        "slack": {"webhook_url": "..."},
        "email": {"to": "foo@bar.com"},
        "sheet": {"id": "SPREADSHEET_ID"},
      }
    """
    if not raw:
        return {}

    if isinstance(raw, dict):
        return raw

    if isinstance(raw, list):
        out: Dict[str, Any] = {}
        for item in raw:
            if not isinstance(item, dict):
                continue

            for key, value in item.items():
                if key == "slack":
                    if isinstance(value, dict):
                        out["slack"] = value
                    else:
                        out["slack"] = {"webhook_url": str(value)}
                elif key == "email":
                    if isinstance(value, dict):
                        out["email"] = value
                    else:
                        out["email"] = {"to": str(value)}
                elif key == "whatsapp":
                    if isinstance(value, dict):
                        out["whatsapp"] = value
                    else:
                        out["whatsapp"] = {"to": str(value)}
                elif key == "sheet":
                    if isinstance(value, dict):
                        out["sheet"] = value
                    else:
                        out["sheet"] = {"id": str(value)}
                else:
                    # Unknown channel key: still keep it, but wrap simple values
                    if isinstance(value, dict):
                        out[key] = value
                    else:
                        out[key] = {"value": value}
        return out

    # Fallback for unexpected shapes
    return {}


async def run_workflows(
    limit_workflows: Optional[int] = None,
    only_workflow_id: Optional[str] = None,
) -> None:
    """
    Main dispatcher:

      1. Fetch active workflows (for metadata + channels).
      2. Optionally filter by `only_workflow_id` (for testing).
      3. Optionally limit count via `limit_workflows`.
      4. Generate all WorkflowEvent via engine.generate_events().
      5. Group events by workflow_id.
      6. For each workflow, dispatch events to enabled channels:
         - Slack
         - Email
         - WhatsApp
         - Sheet
    """
    # Fetch active workflows so we know names, tickers, and channels
    all_workflows = await fetch_active_workflows()

    # Filter by specific workflow id if provided (testing helper)
    if only_workflow_id:
        workflows = [wf for wf in all_workflows if wf.id == only_workflow_id]
    else:
        workflows = all_workflows

    # Apply limit after id-filter, if any
    if limit_workflows is not None:
        workflows = workflows[:limit_workflows]

    if not workflows:
        if only_workflow_id:
            logger.info(
                "No matching workflows found for id=%s (maybe not active?)",
                only_workflow_id,
            )
        else:
            logger.info("No active workflows found, nothing to send.")
        return

    logger.info(
        "Running workflow dispatcher for %d workflows%s",
        len(workflows),
        f" (filtered by id={only_workflow_id})" if only_workflow_id else "",
    )

    # Normalize channels for each workflow
    wf_by_id: Dict[str, Workflow] = {}
    for wf in workflows:
        wf.channels = _normalize_channels(getattr(wf, "channels", None))
        wf_by_id[wf.id] = wf

    # Generate events (engine handles reading MV etc.)
    all_events: List[WorkflowEvent] = await generate_events()

    # Filter events to only the selected workflows
    events_by_wfid: Dict[str, List[WorkflowEvent]] = defaultdict(list)
    for ev in all_events:
        if ev.workflow_id in wf_by_id:
            events_by_wfid[ev.workflow_id].append(ev)

    now = now_wib()
    window_label = fmt_wib_date(now)

    tasks: List[asyncio.Future] = []

    for wfid, wf_events in events_by_wfid.items():
        wf = wf_by_id[wfid]
        if not wf_events:
            continue

        logger.info(
            "Workflow %s (%s) has %d events, dispatching to channels. Channels=%r",
            wf.id,
            wf.name,
            len(wf_events),
            wf.channels,
        )

        channels = wf.channels or {}

        # --- Slack channel ---
        if channels.get("slack") is not None:
            tasks.append(
                asyncio.create_task(
                    send_slack_for_workflow(
                        workflow=wf,
                        events=wf_events,
                        window_start_label=window_label,
                        window_end_label=window_label,
                    )
                )
            )

        # --- Email channel ---
        if send_email_for_workflow is not None and channels.get("email") is not None:
            tasks.append(
                asyncio.create_task(
                    send_email_for_workflow(
                        workflow=wf,
                        events=wf_events,
                        window_start=now,
                        window_end=now,
                    )
                )
            )

        # --- WhatsApp channel ---
        if send_whatsapp_for_workflow is not None and channels.get("whatsapp") is not None:
            tasks.append(
                asyncio.create_task(
                    send_whatsapp_for_workflow(
                        workflow=wf,
                        events=wf_events,
                        window_start_label=window_label,
                        window_end_label=window_label,
                    )
                )
            )

        # --- Sheet channel ---
        if send_sheet_for_workflow is not None and channels.get("sheet") is not None:
            tasks.append(
                asyncio.create_task(
                    send_sheet_for_workflow(
                        workflow=wf,
                        events=wf_events,
                        window_start_label=window_label,
                        window_end_label=window_label,
                    )
                )
            )

    if not tasks:
        logger.info("No events or channels to dispatch.")
        return

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run sectors workflow dispatcher (Slack / Email / WhatsApp / Sheet)."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of workflows processed (for testing).",
    )
    parser.add_argument(
        "--workflow-id",
        type=str,
        default=None,
        help="Run only this workflow id (for testing).",
    )
    args = parser.parse_args()

    asyncio.run(
        run_workflows(
            limit_workflows=args.limit,
            only_workflow_id=args.workflow_id,
        )
    )
