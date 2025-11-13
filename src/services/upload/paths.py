# src/services/io/paths.py
from __future__ import annotations
from pathlib import Path
from typing import Literal, Optional, Dict, List, Tuple

Domain = Literal["filings", "news", "articles"]
Bucket = Literal["in_db", "not_inserted"]

# Default filenames per domain
_DEFAULT_DATA_FILE: Dict[Domain, str] = {
    "filings": "filings_data.json",
    "news": "generated_news.json",
    "articles": "generated_articles.json",
}

# Legacy filenames (your current layout)
_LEGACY_NOT_INSERTED = {
    "idx": "alerts_not_inserted_idx.json",
    "non_idx": "alerts_not_inserted_non_idx.json",
    "low_title_similarity": "low_title_similarity_alerts.json",
    "mix_transfer": "mix_transfer.json",
}
_LEGACY_IN_DB = {
    "idx": "alerts_idx.json",
    "non_idx": "alerts_non_idx.json",
    "correction": "correction_filings.json",
    "suspicious": "suspicious_alerts.json",
}

# Known kinds per bucket
_KINDS_BY_BUCKET: Dict[Bucket, Tuple[str, ...]] = {
    "in_db": ("idx", "non_idx", "correction", "suspicious"),
    "not_inserted": ("idx", "non_idx", "low_title_similarity", "mix_transfer"),
}

def repo_root() -> Path:
    here = Path(__file__).resolve()
    for p in [here, *here.parents]:
        if (p / "src").exists():
            return p
    return Path.cwd()

def ensure_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)

def data_dir(domain: Optional[Domain] = None) -> Path:
    base = repo_root() / "data"
    if domain and (base / domain).exists():
        return base / domain
    return base

def alerts_dir(domain: Domain) -> Path:
    p = repo_root() / "alerts" / domain
    p.mkdir(parents=True, exist_ok=True)
    (p / "history").mkdir(parents=True, exist_ok=True)
    # new buckets
    (p / "in_db").mkdir(parents=True, exist_ok=True)
    (p / "not_inserted").mkdir(parents=True, exist_ok=True)
    return p

def data_file(domain: Domain, filename: Optional[str] = None) -> Path:
    if filename is None:
        filename = _DEFAULT_DATA_FILE[domain]
    sub = data_dir(domain) / filename
    if sub.exists():
        return sub
    return data_dir(None) / filename

def alerts_file_legacy(kind: str, bucket: Bucket) -> Path:
    """Return top-level alerts/<legacy_name>.json"""
    base = repo_root() / "alerts"
    name = (_LEGACY_IN_DB if bucket == "in_db" else _LEGACY_NOT_INSERTED).get(kind)
    return base / name if name else base / f"{kind}.json"

def alerts_file_v2(domain: Domain, bucket: Bucket, kind: str) -> Path:
    """
    New layout: alerts/<domain>/<bucket>/<kind>.json
    Falls back to legacy top-level filename if present.
    """
    base = alerts_dir(domain) / bucket
    newp = base / f"{kind}.json"

    # If legacy exists and new doesn't, return legacy to keep compat
    leg = alerts_file_legacy(kind, bucket)
    if (not newp.exists()) and leg.exists():
        return leg

    ensure_dir(newp)
    return newp

def list_alert_files(domain: Domain, bucket: Bucket) -> List[Path]:
    files: List[Path] = []
    for kind in _KINDS_BY_BUCKET[bucket]:
        p = alerts_file_v2(domain, bucket, kind)
        if p.exists():
            files.append(p)
    return files

def alerts_history_file(domain: Domain, basename: str) -> Path:
    p = alerts_dir(domain) / "history" / basename
    ensure_dir(p)
    return p
