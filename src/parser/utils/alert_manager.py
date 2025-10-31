from __future__ import annotations
import os
import json
import logging
from typing import Optional, Dict, Any, List
from src.common.datetime import timestamp_jakarta 

logger = logging.getLogger(__name__)

def _ensure_parent(path: str) -> None:
    """Create parent directory if the path has one."""
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

class AlertManager:
    """
    Manages writing alert/error files atomically.
    - Keeps an in-memory list of alert dicts.
    - Writes JSON atomically (tmp file + replace).
    """

    def __init__(self, alert_file: str = "alerts/alerts.json", preload_existing: bool = False):
        self.alert_file = alert_file
        self._alerts: List[Dict[str, Any]] = []

        if preload_existing and os.path.exists(self.alert_file):
            try:
                with open(self.alert_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    self._alerts = data
                elif isinstance(data, dict) and isinstance(data.get("alerts"), list):
                    self._alerts = data["alerts"]
                logger.info("Preloaded %d alerts from %s", len(self._alerts), self.alert_file)
            except Exception as e:
                logger.warning("Failed to preload alerts from %s: %s", self.alert_file, e)

    def reset_file(self) -> None:
        """Clear buffer and persist an empty array to the alert file."""
        _ensure_parent(self.alert_file)
        self._alerts = []
        try:

            with open(self.alert_file, "w", encoding="utf-8") as f:
                json.dump([], f, ensure_ascii=False, indent=2)
            logger.info("Reset alert file: %s", self.alert_file)
        except Exception as e:
            logger.error("Failed to reset alert file %s: %s", self.alert_file, e, exc_info=True)

    def log_alert(self, filename: str, reason: str, payload: Optional[Dict[str, Any]] = None) -> None:
        """
        Append one alert to the in-memory buffer.
        NOTE: Call save_alerts() to persist.
        """
        item: Dict[str, Any] = {
            "error_filename": filename,
            "error_reason": reason,
            "timestamp": timestamp_jakarta(), # Use common timestamp
        }
        if payload:
            for k, v in payload.items():
                if k in ("error_filename", "error_reason", "timestamp"):
                    item[f"context_{k}"] = v
                else:
                    item[k] = v
        self._alerts.append(item)

    def save_alerts(self) -> None:
        """Persist alerts to disk atomically (write to .tmp, then replace)."""
        _ensure_parent(self.alert_file)
        tmp = self.alert_file + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._alerts, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.alert_file)
            logger.info("Saved %d alerts to %s", len(self._alerts), self.alert_file)
        except Exception as e:
            logger.error("Error saving alerts to %s: %s", self.alert_file, e, exc_info=True)
            # Best effort: clean tmp on failure
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass