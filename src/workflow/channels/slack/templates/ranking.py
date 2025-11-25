from __future__ import annotations

from typing import List

from src.workflow.models import Workflow, WorkflowEvent
from .base import SlackSectionTemplate


class RankingTemplate(SlackSectionTemplate):
    """
    Generic ranking section (leaders/laggards, volume, value, inst flows).

    It expects each WorkflowEvent.payload["value"] to be a number or simple string.
    """

    def __init__(self, *, title: str, emoji: str, value_label: str) -> None:
        self.title = title
        self.emoji = emoji
        self.value_label = value_label

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
        lines.append(f"*{self.emoji} {self.title}*")
        lines.append("Top names for this metric in your universe:")
        lines.append("")

        MAX_ROWS = 10
        for idx, ev in enumerate(events[:MAX_ROWS], start=1):
            value = (ev.payload or {}).get("value")

            # simple prettify
            if isinstance(value, (int, float)):
                value_str = f"{value:,.2f}"
            else:
                value_str = str(value)

            lines.append(f"{idx}. `{ev.symbol}` – {self.value_label} {value_str}")

        if len(events) > MAX_ROWS:
            lines.append(f"… and *{len(events) - MAX_ROWS}* more.")

        return "\n".join(lines)
