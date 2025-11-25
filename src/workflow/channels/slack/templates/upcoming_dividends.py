from __future__ import annotations

from typing import List

from src.workflow.models import Workflow, WorkflowEvent
from .base import SlackSectionTemplate


class UpcomingDividendsTemplate(SlackSectionTemplate):
    """
    Narrative section for upcoming dividends.

    Uses events with TAG_UPCOMING_DIVIDENDS.
    Expects payload like:
        payload = { "ex_date": "...", "raw": { ...dividend row... } }
    """

    def render(
        self,
        *,
        workflow: Workflow,
        events: List[WorkflowEvent],
        window_start: str,
        window_end: str,
    ) -> str:
        if not events:
            return ""

        lines: List[str] = []
        lines.append("*📈 Dividend Highlight – Upcoming Payouts*")
        lines.append(
            "Upcoming ex-dividend events in your universe. "
            "Useful for income-focused positioning around ex-dates:"
        )
        lines.append("")

        MAX_ROWS = 8
        count = 0

        for ev in events:
            raw = (ev.payload or {}).get("raw") or {}
            ex_date = (ev.payload or {}).get("ex_date") or raw.get("ex_date")
            pay_date = raw.get("payment_date") or raw.get("pay_date")
            div_per_share = raw.get("dividend_amount") or raw.get("div_per_share")
            company = raw.get("company_name") or raw.get("company")

            parts = [f"`{ev.symbol}`"]
            if company:
                parts.append(f"– {company}")

            detail_parts = []
            if div_per_share is not None:
                detail_parts.append(f"div/share {div_per_share}")
            if ex_date:
                detail_parts.append(f"ex {ex_date}")
            if pay_date:
                detail_parts.append(f"pay {pay_date}")

            if detail_parts:
                parts.append("(" + ", ".join(detail_parts) + ")")

            lines.append("• " + " ".join(parts))
            count += 1
            if count >= MAX_ROWS:
                break

        remaining = len(events) - count
        if remaining > 0:
            lines.append(f"… and *{remaining}* more dividend entries in this window.")

        return "\n".join(lines)
