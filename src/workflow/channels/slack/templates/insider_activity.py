from __future__ import annotations

from typing import List, Any, Dict

from src.workflow.models import Workflow, WorkflowEvent


def _parse_json_array(value: Any) -> List[Dict[str, Any]]:
    """
    Small helper: if Supabase returns text JSON instead of list, parse it.
    Safe fallback: return [] on any error.
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        return [value]

    import json

    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
        return []
    except Exception:
        return []


def render_insider_activity_section(
    *,
    workflow: Workflow,
    buy_events: List[WorkflowEvent],
    sell_events: List[WorkflowEvent],
    window_start: str,
    window_end: str,
) -> str:
    """
    Combined narrative section for insider BUY + SELL activity.

    This mirrors the previous _render_insider_section but moved into a
    dedicated template module.
    """
    total_buys = len(buy_events)
    total_sells = len(sell_events)

    if not total_buys and not total_sells:
        return ""

    lines: List[str] = []
    lines.append("*🔍 Insider Trading Activity*")

    # Summary line
    summary_bits: List[str] = []
    if total_buys:
        summary_bits.append(f"{total_buys} BUY")
    if total_sells:
        summary_bits.append(f"{total_sells} SELL")

    if summary_bits:
        lines.append(
            "We detected "
            + ", ".join(summary_bits)
            + " insider transaction(s) in your universe this period:"
        )
        lines.append("")

    def _render_side(events: List[WorkflowEvent], label: str, emoji: str) -> List[str]:
        side_lines: List[str] = []
        if not events:
            return side_lines

        side_lines.append(f"{emoji} *{label}*")
        MAX_ROWS = 6
        count = 0

        for ev in events:
            value = (ev.payload or {}).get("value")
            rows = _parse_json_array(value) or [{}]

            for row in rows:
                if not isinstance(row, dict):
                    # generic fallback
                    side_lines.append(f"• `{ev.symbol}` – {value}")
                    count += 1
                    if count >= MAX_ROWS:
                        break
                    continue

                holder = row.get("holder_name") or row.get("holder") or "unknown holder"
                tx_value = row.get("transaction_value") or row.get("value")
                amount = row.get("amount")
                pct = row.get("share_percentage_transaction") or row.get("pct_tx")
                when = row.get("display_time") or row.get("time")

                pieces = [f"`{ev.symbol}` – {holder}"]
                tail_parts = []

                if tx_value is not None:
                    tail_parts.append(f"val {tx_value}")
                if amount is not None:
                    tail_parts.append(f"{amount} sh")
                if pct is not None:
                    tail_parts.append(f"{pct}%")
                if when:
                    tail_parts.append(f"at {when}")

                if tail_parts:
                    pieces.append("(" + ", ".join(tail_parts) + ")")

                side_lines.append("• " + " ".join(pieces))
                count += 1
                if count >= MAX_ROWS:
                    break
            if count >= MAX_ROWS:
                break

        if count and len(events) > count:
            side_lines.append(f"… and *{len(events) - count}* more {label.lower()} entries.")

        return side_lines

    buy_lines = _render_side(buy_events, "Insider BUY", "🟢")
    sell_lines = _render_side(sell_events, "Insider SELL", "🔴")

    if buy_lines:
        lines.extend(buy_lines)
    if sell_lines:
        if buy_lines:
            lines.append("")  # blank line separator
        lines.extend(sell_lines)

    return "\n".join(lines)
