from __future__ import annotations
import argparse
import json
import logging
import os
from pathlib import Path
from typing import Iterable, Tuple, Dict, List
import shutil

log = logging.getLogger("alerts.bucketize")

DEFAULT_FROM_DIR = "alerts"

# mapping file yang diharapkan ada pada folder sumber
INSERTED_CANDIDATES: List[Tuple[str, str]] = [
    ("alerts_idx.json", "alerts_idx.json"),
    ("alerts_non_idx.json", "alerts_non_idx.json"),
    ("correction_filings.json", "correction_filings.json"),
    # alias: beberapa pipeline lama tulis "inconsistent_alerts.json"
    ("suspicious_alerts.json", "suspicious_alerts.json"),
    ("inconsistent_alerts.json", "suspicious_alerts.json"),  # alias -> disimpan sebagai suspicious_alerts.json
]

NOT_INSERTED_CANDIDATES: List[Tuple[str, str]] = [
    ("alerts_not_inserted_idx.json", "alerts_not_inserted_idx.json"),
    ("alerts_not_inserted_non_idx.json", "alerts_not_inserted_non_idx.json"),
    ("low_title_similarity_alerts.json", "low_title_similarity_alerts.json"),
]


def _json_nonempty(p: Path) -> bool:
    """True bila file ada dan isinya bukan [] / {} / kosong."""
    if not p.exists():
        return False
    try:
        if p.stat().st_size == 0:
            return False
    except Exception:
        return False

    try:
        raw = p.read_text(encoding="utf-8")
        if not raw.strip():
            return False
        data = json.loads(raw)
        if isinstance(data, (list, dict)):
            return len(data) > 0
        return True  # kalau bukan JSON list/dict tapi ada isinya, anggap non-empty
    except Exception:
        # bukan JSON valid -> kalau file ada & size>0 anggap non-empty
        return True


def _ensure_dir(d: Path) -> None:
    d.mkdir(parents=True, exist_ok=True)


def _copy_if_nonempty(src: Path, dst: Path, dry_run: bool = False) -> bool:
    if not _json_nonempty(src):
        log.info("skip (empty): %s", src)
        return False
    _ensure_dir(dst.parent)
    if dry_run:
        log.info("[DRY RUN] would copy %s -> %s", src, dst)
        return True
    shutil.copy2(src, dst)
    log.info("copied: %s -> %s", src, dst)
    return True


def bucketize(
    from_dir: Path,
    inserted_dir: Path,
    not_inserted_dir: Path,
    *,
    dry_run: bool = False,
) -> Dict[str, int]:
    """
    Pindahkan (copy) file alert non-kosong dari from_dir ke dua bucket:
    - inserted_dir: alerts_idx.json, alerts_non_idx.json, correction_filings.json, suspicious_alerts.json
      (alias accepted: inconsistent_alerts.json -> suspicious_alerts.json)
    - not_inserted_dir: alerts_not_inserted_idx.json, alerts_not_inserted_non_idx.json, low_title_similarity_alerts.json
    """
    from_dir = from_dir.resolve()
    inserted_dir = inserted_dir.resolve()
    not_inserted_dir = not_inserted_dir.resolve()

    stats = {"inserted": 0, "not_inserted": 0}

    # inserted
    for src_name, final_name in INSERTED_CANDIDATES:
        src = from_dir / src_name
        dst = inserted_dir / final_name
        if _copy_if_nonempty(src, dst, dry_run=dry_run):
            stats["inserted"] += 1

    # not inserted
    for src_name, final_name in NOT_INSERTED_CANDIDATES:
        src = from_dir / src_name
        dst = not_inserted_dir / final_name
        if _copy_if_nonempty(src, dst, dry_run=dry_run):
            stats["not_inserted"] += 1

    return stats


def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Bucketize alerts into alerts_inserted/ and alerts_not_inserted/")
    p.add_argument("--from", dest="from_dir", default=DEFAULT_FROM_DIR,
                   help="Source alerts dir (default: alerts)")
    p.add_argument("--inserted-dir", default="alerts_inserted",
                   help="Destination dir for inserted alerts (default: alerts_inserted)")
    p.add_argument("--not-inserted-dir", default="alerts_not_inserted",
                   help="Destination dir for not-inserted alerts (default: alerts_not_inserted)")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    return p


def main():
    ap = _build_argparser()
    args = ap.parse_args()

    logging.basicConfig(
        level=(logging.DEBUG if args.verbose else logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    stats = bucketize(
        from_dir=Path(getattr(args, "from_dir", DEFAULT_FROM_DIR)),
        inserted_dir=Path(args.inserted_dir),
        not_inserted_dir=Path(args.not_inserted_dir),
        dry_run=args.dry_run,
    )
    log.info("done. inserted=%d, not_inserted=%d", stats["inserted"], stats["not_inserted"])


if __name__ == "__main__":
    main()
