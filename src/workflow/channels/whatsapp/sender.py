from __future__ import annotations

from typing import Any, Dict, List

from twilio.base.exceptions import TwilioRestException
from twilio.rest import Client

from src.services.whatsapp.utils.config import (
    ACCOUNT_SID,
    AUTH_TOKEN,
    TWILIO_FROM_NUMBER,
    LOGGER,
    TEMPLATE_SID
)
from src.workflow.models import Workflow, WorkflowEvent
from .formatter import build_whatsapp_digest

import json 
import time 
import random 


async def send_whatsapp_for_workflow(
    workflow: Workflow,
    events: List[WorkflowEvent],
    ctx: Dict[str, Any],
) -> None:
    """
    Send a single WhatsApp digest message for one workflow using Twilio.

    Expected shape of workflow.channels["whatsapp"] (example):

    {
      "enabled": true,
      "to_numbers": ["+62812xxxxxxx", "+6598xxxxxxx"]
      # or "to_number": "+62812xxxxxxx"
    }
    """
    # Check if WhatsApp is enabled for this workflow
    cfg = (workflow.channels or {}).get("whatsapp") or {}
    if not cfg.get("enabled", True):
        LOGGER.info("WhatsApp disabled for workflow %s", workflow.id)
        return

    # Get recipients (handle both string and list formats)
    to_numbers = cfg.get("to_numbers") or cfg.get("to_number")
    if isinstance(to_numbers, str):
        to_numbers = [to_numbers]

    if not to_numbers:
        LOGGER.warning("No WhatsApp recipients for workflow %s", workflow.id)
        return

    payloads = build_whatsapp_digest(workflow, events, ctx)
    if not payloads:
        LOGGER.info(
            "No WhatsApp content for workflow %s (no events for subscribed tags)",
            workflow.id,
        )
        return

    if not ACCOUNT_SID or not AUTH_TOKEN or not TWILIO_FROM_NUMBER or not TEMPLATE_SID:
        LOGGER.error("Twilio config missing, cannot send WhatsApp for workflow %s", workflow.id)
        return

    # Initialize Twilio Client
    try:
        client = Client(ACCOUNT_SID, AUTH_TOKEN)
    except Exception as error:
        LOGGER.error(f"Twilio Init Error: {error}")
        return None 

    for number_recepient in to_numbers:
        for payload in payloads: 
            try:
                msg = client.messages.create(
                    from_=f"whatsapp:{TWILIO_FROM_NUMBER}",
                    to=f"whatsapp:{number_recepient}",
                    content_sid=TEMPLATE_SID,
                    content_variables=json.dumps(payload)
                )

                time.sleep(random.uniform(1, 2.5))

                LOGGER.info(
                    "Sent WhatsApp workflow digest to %s (sid=%s) for workflow %s",
                    number_recepient,
                    msg.sid,
                    workflow.id,
                )

            except TwilioRestException as exc:
                LOGGER.error(
                    "Failed to send WhatsApp to %s for workflow %s: %s",
                    number_recepient,
                    workflow.id,
                    exc,
                    exc_info=True,
                )
            except Exception as error:
                LOGGER.error(
                    f"Failed to send WhatsApp to %s for workflow %s: %s: {error}"
                )
                