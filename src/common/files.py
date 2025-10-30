"""File utilities (py39 compatible)."""

from __future__ import annotations
import re
import os
from pathlib import Path
from typing import Union, Optional, Any


PathLike = Union[str, Path]

# Precompiled pattern to remove/replace unsafe filename chars
_UNSAFE = re.compile(r'[^A-Za-z0-9._-]+')


def ensure_dir(p: PathLike) -> Path:
    """Create directory if missing; return the Path."""
    path = Path(p)
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_parent(p: PathLike) -> Path:
    """Ensure parent directory exists for given file path; return the file Path."""
    path = Path(p)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def safe_filename_from_url(url: str, default: str = "file.pdf") -> str:
    """Derive a safe filename from URL (keep extension if present)."""
    s = str(url or "").strip()
    if not s:
        return default

    # Take the last path segment before query/fragment
    basename = s.split("?")[0].split("#")[0].rstrip("/").split("/")[-1] or default

    # If name is too short or looks like query-only, fallback
    if not basename or "." not in basename:
        basename = default

    # Sanitize
    name, dot, ext = basename.partition(".")
    name = _UNSAFE.sub("-", name).strip("-").lower() or "file"
    ext = _UNSAFE.sub("", ext).lower()

    # Reassemble
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
    return Path(path).read_text(encoding=encoding)


def read_json(path: PathLike, encoding: str = "utf-8") -> Any:
    """Read JSON file into Python object."""
    import json
    return json.loads(read_text(path, encoding=encoding))
