# src/workflow/channels/slack/sender.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
import os
import httpx

from src.common.log import get_logger
from src.workflow.models import Workflow, WorkflowEvent
from .formatter import build_slack_payload_for_workflow

logger = get_logger("workflow.slack.sender")


def _get_slack_webhook_url(workflow: Workflow) -> Optional[str]:
    """
    Resolve the Slack webhook URL for a workflow.

    Supports several shapes of `workflow.channels`:
      - {"slack": "https://hooks.slack.com/..."}
      - {"slack": {"webhook_url": "https://hooks.slack.com/..."}}
      - {"slack": {"url": "..."}}
      - {"slack": {"value": "..."}}
      - Fallback: env var SLACK_WORKFLOW_WEBHOOK
    """
    channels = getattr(workflow, "channels", None) or {}

    slack_cfg: Any = None
    if isinstance(channels, dict):
        slack_cfg = channels.get("slack")

    # Case 1: plain string → treat as URL
    if isinstance(slack_cfg, str):
        url = slack_cfg.strip()
        if url:
            return url

    # Case 2: dict → try several common keys
    if isinstance(slack_cfg, dict):
        for key in ("webhook_url", "url", "value"):
            val = slack_cfg.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()

    # Fallback: environment variable
    env_url = os.getenv("SLACK_WORKFLOW_WEBHOOK")
    if env_url and env_url.strip():
        return env_url.strip()

    return None


async def send_slack_for_workflow(
    *,
    workflow: Workflow,
    events: List[WorkflowEvent],
    window_start: str,
    window_end: str,
) -> None:
    # Resolve webhook URL in a robust way
    webhook_url: Optional[str] = _get_slack_webhook_url(workflow)

    if not webhook_url:
        logger.info(
            "Workflow %s (%s) has no slack webhook configured, skipping. Channels: %r",
            workflow.id,
            workflow.name,
            getattr(workflow, "channels", None),
        )
        return

    if not events:
        logger.info(
            "Workflow %s (%s) has no events for Slack, skipping",
            workflow.id,
            workflow.name,
        )
        return

    payload = build_slack_payload_for_workflow(
        workflow=workflow,
        events=events,
        window_start=window_start,
        window_end=window_end,
    )

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(webhook_url, json=payload)
            resp.raise_for_status()
    except Exception as exc:
        logger.error(
            "Failed to send Slack message for workflow %s (%s): %s",
            workflow.id,
            workflow.name,
            exc,
        )
    else:
        logger.info(
            "Sent Slack digest for workflow %s (%s) with %d events",
            workflow.id,
            workflow.name,
            len(events),
        )
