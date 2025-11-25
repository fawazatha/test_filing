from __future__ import annotations

from typing import Protocol, List
from src.workflow.models import Workflow, WorkflowEvent


class SlackSectionTemplate(Protocol):
    """
    Base protocol for per-tag Slack templates.
    Each template renders one narrative section in mrkdwn.
    """

    def render(
        self,
        *,
        workflow: Workflow,
        events: List[WorkflowEvent],
        window_start: str,
        window_end: str,
    ) -> str:
        """
        Return mrkdwn text for this section.
        Return empty string / whitespace-only if nothing should be shown.
        """
        ...
