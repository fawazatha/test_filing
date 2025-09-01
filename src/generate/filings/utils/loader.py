from __future__ import annotations
from typing import List, Dict, Any
import json
from pathlib import Path

def load_idx_dicts(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    return json.loads(path.read_text())

def load_non_idx_dicts(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    return json.loads(path.read_text())
