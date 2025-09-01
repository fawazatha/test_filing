import os
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)


class AlertManager:
    
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
                logger.info(f"Preloaded {len(self._alerts)} alerts from {self.alert_file}")
                
            except Exception as e:
                logger.warning(f"Failed to preload alerts from {self.alert_file}: {e}")

    def reset_file(self) -> None:
        os.makedirs(os.path.dirname(self.alert_file), exist_ok=True)
        self._alerts = []
        try:
            with open(self.alert_file, "w", encoding="utf-8") as f:
                json.dump([], f, ensure_ascii=False, indent=2)
            logger.info(f"Reset alert file: {self.alert_file}")
        except Exception as e:
            logger.error(f"Failed to reset alert file {self.alert_file}: {e}")

    def log_alert(self, filename: str, reason: str, payload: Optional[Dict[str, Any]] = None) -> None:
        item: Dict[str, Any] = {
            "error_filename": filename,
            "error_reason": reason,
            "timestamp": datetime.now().isoformat(timespec="microseconds") + "Z",
        }
        if payload:
            item.update(payload)
        self._alerts.append(item)

    def save_alerts(self) -> None:
        try:
            os.makedirs(os.path.dirname(self.alert_file), exist_ok=True)
            tmp = self.alert_file + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._alerts, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.alert_file)  # atomic replace
            logger.info(f"Saved {len(self._alerts)} alerts to {self.alert_file}")
        except Exception as e:
            logger.error(f"Error saving alerts to {self.alert_file}: {e}")
