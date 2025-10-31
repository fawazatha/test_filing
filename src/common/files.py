from __future__ import annotations
import os
import re
import json
import shutil
from pathlib import Path
from typing import Union, Optional, Any

"""
File and I/O utilities.
Merged 'io.py' and 'files.py' to remove redundancy.
"""

PathLike = Union[str, Path]

# Guard: refuse destructive ops on these
_DANGEROUS = {
    Path("/").resolve(),
    Path.home().resolve(),
    Path.cwd().resolve(),
}

# Precompiled pattern to remove/replace unsafe filename chars
_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


def _to_path(p: PathLike) -> Path:
    """Normalize to Path and expand user (~)."""
    return Path(p).expanduser()


def ensure_clean_dir(path: PathLike) -> Path:
    """
    Remove the directory if exists, then recreate it empty.
    Refuses to operate on dangerous paths like '/', HOME, or CWD.
    """
    p = _to_path(path).resolve()
    if p in _DANGEROUS:
        raise ValueError(f"Refusing to clean dangerous path: {p}")
    if p.exists():
        shutil.rmtree(p)
    p.mkdir(parents=True, exist_ok=True)
    return p


def safe_unlink(path: PathLike) -> bool:
    """
    Delete a file if it exists. Create parent dir so later writes won't fail.
    Returns True if a file was removed, False otherwise.
    """
    p = _to_path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists():
        p.unlink()
        return True
    return False


def ensure_dir(p: PathLike) -> Path:
    """Create directory if missing; return the Path."""
    path = _to_path(p)
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_parent(p: PathLike) -> Path:
    """Ensure parent directory exists for given file path; return the file Path."""
    path = _to_path(p)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def safe_filename_from_url(url: str, default: str = "file.pdf") -> str:
    """
    Derive a safe filename from a URL (keep extension if present).
    - Sanitizes base name.
    - Preserves only the last extension (e.g., 'a.b.c.pdf' -> '.pdf').
    - Falls back to `default` if the URL doesn't provide a meaningful name.
    """
    s = (url or "").strip()
    if not s:
        return default

    # Take the last path segment before query/fragment
    basename = s.split("?", 1)[0].split("#", 1)[0].rstrip("/").split("/")[-1] or default

    # If name is too short or looks like query-only, fallback
    if not basename or basename in {".", ".."}:
        basename = default

    # Split on the LAST dot only
    if "." in basename:
        name, ext = basename.rsplit(".", 1)
        ext = _UNSAFE.sub("", ext).lower()
    else:
        name, ext = basename, ""

    # Sanitize name
    name = _UNSAFE.sub("-", name).strip("-").lower() or "file"

    # Reassemble (keep ext if any)
    return f"{name}.{ext}" if ext else name


def write_text(path: PathLike, content: str, encoding: str = "utf-8") -> Path:
    """Write text atomically (best-effort) and return the Path."""
    p = ensure_parent(path)
    tmp = Path(str(p) + ".tmp")
    tmp.write_text(content, encoding=encoding)
    os.replace(tmp, p)
    return p


def write_bytes(path: PathLike, data: bytes) -> Path:
    """Write bytes atomically (best-effort) and return the Path."""
    p = ensure_parent(path)
    tmp = Path(str(p) + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, p)
    return p


def read_text(path: PathLike, encoding: str = "utf-8") -> str:
    """Read text content."""
    return _to_path(path).read_text(encoding=encoding)


def read_json(path: PathLike, encoding: str = "utf-8") -> Any:
    """Read JSON file into Python object."""
    return json.loads(read_text(path, encoding=encoding))


def atomic_write_json(path: PathLike, obj: Any, encoding: str = "utf-8") -> Path:
    """
    Write an object to JSON atomically (via write_text).
    Uses default=str to avoid failure on non-serializable types (e.g., Path, datetime).
    """
    content = json.dumps(obj, ensure_ascii=False, indent=2, default=str)
    return write_text(path, content, encoding=encoding)
