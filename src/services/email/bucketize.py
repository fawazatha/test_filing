# services/email/bucketize.py
from __future__ import annotations
import argparse
import json
import logging
import shutil
from pathlib import Path
from typing import Dict, List, Tuple

log = logging.getLogger("alerts.bucketize")

DEFAULT_FROM_DIR = "alerts"

# === Legacy/static filenames (dipertahankan untuk kompatibilitas) ===
INSERTED_CANDIDATES: List[Tuple[str, str]] = [
    ("alerts_idx.json", "alerts_idx.json"),
    ("alerts_non_idx.json", "alerts_non_idx.json"),
    ("correction_filings.json", "correction_filings.json"),
    # alias lama: "inconsistent_alerts.json" → simpan sebagai suspicious_alerts.json
    ("suspicious_alerts.json", "suspicious_alerts.json"),
    ("inconsistent_alerts.json", "suspicious_alerts.json"),
]

NOT_INSERTED_CANDIDATES: List[Tuple[str, str]] = [
    ("alerts_not_inserted_idx.json", "alerts_not_inserted_idx.json"),
    ("alerts_not_inserted_non_idx.json", "alerts_not_inserted_non_idx.json"),
    ("low_title_similarity_alerts.json", "low_title_similarity_alerts.json"),
]

# === V2 dynamic filenames (dihasilkan oleh step_alerts_v2_from_filings) ===
V2_INSERTED_GLOB = "alerts_inserted_*.json"
V2_NOT_INSERTED_GLOB = "alerts_not_inserted_*.json"


def _json_nonempty(p: Path) -> bool:
    """True bila file ada & isinya bukan kosong. Deteksi list/dict kosong,
    serta dict dengan kunci umum ('alerts', 'rows', 'data', 'items', 'results')."""
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
    except Exception:
        # bukan JSON valid -> selama size>0, anggap non-empty
        return True

    if isinstance(data, list):
        return len(data) > 0
    if isinstance(data, dict):
        # Cek beberapa kunci yang lazim menampung array
        for k in ("alerts", "rows", "data", "items", "results"):
            v = data.get(k)
            if isinstance(v, list):
                return len(v) > 0
        # Jika bukan itu, anggap non-empty bila dict punya isi
        return len(data) > 0
    # tipe lain (string/number/bool): anggap non-empty bila ada
    return True


def _ensure_dir(d: Path) -> None:
    d.mkdir(parents=True, exist_ok=True)


def _copy_if_nonempty(src: Path, dst: Path, *, dry_run: bool = False) -> bool:
    if not _json_nonempty(src):
        log.info("skip (empty): %s", src)
        return False
    _ensure_dir(dst.parent)
    if dry_run:
        log.info("[DRY RUN] would copy %s -> %s", src, dst)
        return True
    # gunakan copy2 agar metadata waktu ikut
    shutil.copy2(src, dst)
    log.info("copied: %s -> %s", src, dst)
    return True


def bucketize(
    *,
    from_dir: Path,
    inserted_dir: Path,
    not_inserted_dir: Path,
    dry_run: bool = False,
) -> Dict[str, int]:
    """
    Salin file alert non-kosong dari `from_dir` ke dua bucket:
      - `inserted_dir`
      - `not_inserted_dir`

    Mendukung:
      1) Legacy/static filenames (INSERTED_CANDIDATES & NOT_INSERTED_CANDIDATES)
      2) V2 dynamic (glob):
           alerts_inserted_*.json       -> inserted_dir
           alerts_not_inserted_*.json   -> not_inserted_dir
    """
    from_dir = from_dir.resolve()
    inserted_dir = inserted_dir.resolve()
    not_inserted_dir = not_inserted_dir.resolve()

    stats = {"inserted": 0, "not_inserted": 0}

    # ---- 1) Legacy/static ----
    for src_name, final_name in INSERTED_CANDIDATES:
        src = from_dir / src_name
        dst = inserted_dir / final_name
        if _copy_if_nonempty(src, dst, dry_run=dry_run):
            stats["inserted"] += 1

    for src_name, final_name in NOT_INSERTED_CANDIDATES:
        src = from_dir / src_name
        dst = not_inserted_dir / final_name
        if _copy_if_nonempty(src, dst, dry_run=dry_run):
            stats["not_inserted"] += 1

    # ---- 2) V2 dynamic (glob) ----
    # Pertahankan nama asli (agar tanggal terbaca di filename tujuan)
    for p in sorted(from_dir.glob(V2_INSERTED_GLOB)):
        dst = inserted_dir / p.name
        if _copy_if_nonempty(p, dst, dry_run=dry_run):
            stats["inserted"] += 1

    for p in sorted(from_dir.glob(V2_NOT_INSERTED_GLOB)):
        dst = not_inserted_dir / p.name
        if _copy_if_nonempty(p, dst, dry_run=dry_run):
            stats["not_inserted"] += 1

    return stats


def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Bucketize alerts into alerts_inserted/ and alerts_not_inserted/ (supports legacy & V2 filenames)"
    )
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
    log.info("[BUCKETIZE] inserted=%d not_inserted=%d", stats["inserted"], stats["not_inserted"])


if __name__ == "__main__":
    main()
