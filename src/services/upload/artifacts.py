from __future__ import annotations
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable, List, Dict, Tuple, Optional
from datetime import datetime, timezone
import hashlib, json, os, zipfile

_CHUNK = 1024 * 1024

@dataclass
class FileEntry:
    path: str          # path relatif di dalam zip
    size: int
    sha256: str
    mtime: str         # ISO8601

@dataclass
class Manifest:
    created_at: str
    base_dir: str
    prefix: str
    total_files: int
    total_size: int
    files: List[FileEntry]

def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        while True:
            b = f.read(_CHUNK)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def _to_posix(path: Path) -> str:
    return str(path.as_posix())

def _match_globs(patterns: Iterable[str]) -> List[Path]:
    out: List[Path] = []
    for pat in patterns or []:
        # support recursive globs (**)
        p = Path(".")
        found = sorted(p.glob(pat))
        out.extend([x for x in found if x.is_file()])
    # unique, keep order
    seen = set()
    uniq: List[Path] = []
    for f in out:
        s = os.path.abspath(f)
        if s in seen:
            continue
        seen.add(s)
        uniq.append(f)
    return uniq

def _filter_excludes(paths: List[Path], exclude_patterns: Iterable[str] | None) -> List[Path]:
    if not exclude_patterns:
        return paths
    excl: List[Path] = []
    for pat in exclude_patterns:
        excl.extend(Path(".").glob(pat))
    excl_abs = {os.path.abspath(x) for x in excl}
    return [p for p in paths if os.path.abspath(p) not in excl_abs]

def collect_files(
    include_patterns: Iterable[str],
    exclude_patterns: Iterable[str] | None = None,
) -> List[Path]:
    files = _match_globs(include_patterns)
    files = _filter_excludes(files, exclude_patterns)
    return files

def make_artifact_name(prefix: str, tag: Optional[str] = None) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}{('_' + tag) if tag else ''}.zip"

def make_artifact_zip(
    *,
    prefix: str = "filings",
    patterns: Iterable[str] | None = None,
    exclude_patterns: Iterable[str] | None = None,
    out_dir: str | Path = "artifacts",
    base_dir: str | Path = ".",
    manifest_name: str = "MANIFEST.json",
) -> Tuple[Path, Manifest]:
    """
    Kumpulkan file via glob patterns -> zip -> tambahkan MANIFEST.json.
    Return: (zip_path, manifest_obj)
    """
    base = Path(base_dir).resolve()
    outd = Path(out_dir)
    outd.mkdir(parents=True, exist_ok=True)

    files = collect_files(patterns or [], exclude_patterns or [])
    entries: List[FileEntry] = []
    total_size = 0

    zip_name = make_artifact_name(prefix)
    zip_path = outd / zip_name

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for fp in files:
            rp = fp.resolve()
            rel = rp.relative_to(base)
            stat = rp.stat()
            sha = _sha256(rp)
            total_size += stat.st_size
            entries.append(
                FileEntry(
                    path=_to_posix(rel),
                    size=stat.st_size,
                    sha256=sha,
                    mtime=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                )
            )
            zf.write(rp, arcname=str(rel))
        # write manifest at the end
        manifest = Manifest(
            created_at=_now_iso(),
            base_dir=str(base),
            prefix=prefix,
            total_files=len(entries),
            total_size=total_size,
            files=entries,
        )
        zf.writestr(manifest_name, json.dumps(asdict(manifest), ensure_ascii=False, indent=2))

    return zip_path, manifest
