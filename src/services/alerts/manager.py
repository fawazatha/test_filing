from __future__ import annotations

import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional

__all__ = ["AlertManager"]

class AlertManager:
    """
    Dipakai di CLI upload:
      am = AlertManager("filings")
      am.record({...}, inserted=True/False)
      am.flush()
      am.rotate_snapshot(tag="upload")
    """

    def __init__(self, kind: str, base_dir: Optional[Path] = None) -> None:
        self.kind = kind
        self.base_dir = Path(base_dir) if base_dir else Path("alerts")
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._buf_in_db: List[Dict[str, Any]] = []
        self._buf_not_inserted: List[Dict[str, Any]] = []
        self.path_in_db = self.base_dir / f"{self.kind}_in_db.json"
        self.path_not_inserted = self.base_dir / f"{self.kind}_not_inserted.json"

    def record(self, obj: Dict[str, Any], inserted: bool) -> None:
        (self._buf_in_db if inserted else self._buf_not_inserted).append(obj)

    def _write_or_remove(self, path: Path, data: List[Dict[str, Any]]) -> None:
        if data:
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(path)
        else:
            if path.exists(): path.unlink()

    def flush(self) -> None:
        self._write_or_remove(self.path_in_db, self._buf_in_db)
        self._write_or_remove(self.path_not_inserted, self._buf_not_inserted)

    def rotate_snapshot(self, tag: str = "snapshot") -> None:
        snap_dir = self.base_dir / "snapshots"
        snap_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        if self.path_in_db.exists():
            shutil.copy2(self.path_in_db, snap_dir / f"{ts}__{self.kind}__{tag}__in_db.json")
        if self.path_not_inserted.exists():
            shutil.copy2(self.path_not_inserted, snap_dir / f"{ts}__{self.kind}__{tag}__not_inserted.json")
