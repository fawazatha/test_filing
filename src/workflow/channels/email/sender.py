# src/workflow/channels/email/sender.py
from __future__ import annotations

from typing import List, Dict, Optional
from datetime import datetime
import os

from jinja2 import Environment, FileSystemLoader, select_autoescape

from src.common.log import get_logger
from src.workflow.models import Workflow, WorkflowEvent
from src.workflow.channels.email.context import build_email_context_from_events
from src.services.email.mailer import send_attachments  

logger = get_logger("workflow.email.sender")

TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "template")
env = Environment(
    loader=FileSystemLoader(TEMPLATES_DIR),
    autoescape=select_autoescape(["html", "xml"]),
)

def _format_abbrev(value):
    """
    1_200 → 1.2K, 1_500_000 → 1.5M, dst.
    """
    if value is None:
        return "-"
    try:
        n = float(value)
    except (TypeError, ValueError):
        return str(value)

    abs_n = abs(n)
    for unit, factor in [("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)]:
        if abs_n >= factor:
            v = n / factor
            # 1.0K → 1K
            s = f"{v:.1f}".rstrip("0").rstrip(".")
            return f"{s}{unit}"
    return f"{n:,.0f}".replace(",", ".")


def _format_idr(value):
    """
    1234567 → 'Rp 1.234.567'
    """
    if value is None:
        return "-"
    try:
        n = float(value)
    except (TypeError, ValueError):
        return str(value)
    if n.is_integer():
        s = f"{int(n):,}".replace(",", ".")
    else:
        s = f"{n:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"Rp {s}"


def _format_idr_abbrev(value):
    """
    1500000 → 'Rp 1.5M'
    """
    if value is None:
        return "-"
    try:
        n = float(value)
    except (TypeError, ValueError):
        return str(value)

    # Reuse logic abbreviate
    abs_n = abs(n)
    for unit, factor in [("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)]:
        if abs_n >= factor:
            v = n / factor
            s = f"{v:.1f}".rstrip("0").rstrip(".")
            return f"Rp {s}{unit}"
    return _format_idr(n)


env.filters["format_abbrev"] = _format_abbrev
env.filters["format_idr"] = _format_idr
env.filters["format_idr_abbrev"] = _format_idr_abbrev


def _resolve_email_cfg(workflow: Workflow) -> Dict[str, Optional[str]]:
    """
    Resolve email recipients from workflow.channels['email'].

    Supported shapes:
      - "email": "user@example.com"
      - "email": {"to": "user@example.com"}
      - "email": {"value": "user@example.com", "cc": "...", "bcc": "..."}
    """
    channels = getattr(workflow, "channels", None) or {}
    raw = channels.get("email") or {}

    # If channel is stored as plain string
    if isinstance(raw, str):
        return {"to": raw, "cc": None, "bcc": None}

    if not isinstance(raw, dict):
        return {"to": None, "cc": None, "bcc": None}

    # Prefer "to", but fall back to "value" (per logged payloads: {'value': '...'})
    to = raw.get("to") or raw.get("value")
    cc = raw.get("cc")
    bcc = raw.get("bcc")
    return {"to": to, "cc": cc, "bcc": bcc}


async def send_email_for_workflow(
    workflow: Workflow,
    events: List[WorkflowEvent],
    window_start: datetime,
    window_end: datetime,
) -> None:
    """
    Build context -> render HTML template -> send via SES.

    This is called once per workflow by the runner.
    """
    ctx = build_email_context_from_events(workflow, events, window_start, window_end)

    # If context only has meta keys, skip sending
    meta_keys = {"window_start", "window_end", "as_of", "period"}
    if len(set(ctx.keys()) - meta_keys) == 0:
        logger.info(
            "Workflow %s (%s) has no sections for email digest, skipping",
            workflow.id,
            workflow.name,
        )
        return

    # Render HTML using your workflow_template.html
    template = env.get_template("workflow_template.html")
    body_html = template.render(**ctx)
    body_text = f"Market Digest {ctx['as_of']} — open this email in HTML-capable client."
    subject = f"Market Digest - {ctx['as_of']}"

    cfg = _resolve_email_cfg(workflow)
    to = cfg["to"]
    if not to:
        logger.info(
            "Workflow %s (%s) has email channel but no recipient configured, skipping",
            workflow.id,
            workflow.name,
        )
        return

    logger.info(
        "Sending Market Digest email for workflow %s (%s) to %s",
        workflow.id,
        workflow.name,
        to,
    )

    # send_attachments is synchronous; OK to call from async function
    send_attachments(
        to=to,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
        files=[],       # no attachments yet
        cc=cfg["cc"],
        bcc=cfg["bcc"],
        aws_region=None,
    )
