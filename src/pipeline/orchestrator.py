from __future__ import annotations
import argparse
import json
import logging
import os
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple


try:
    import zoneinfo
    JKT = zoneinfo.ZoneInfo("Asia/Jakarta")
except Exception: 
    JKT = None

from scripts.company_map_hybrid import get_company_map

from ingestion.runner import (
    get_ownership_announcements,
    get_ownership_announcements_range,
    save_json as save_ann_json,
)
from downloader.runner import download_pdfs
from models.announcement import Announcement

from parser import parser_idx as parser_idx_mod
from parser import parser_non_idx as parser_non_idx_mod

from generate.filings.runner import run as run_generate
from services.alerts.bucketize import bucketize as bucketize_alerts
from services.io.artifacts import make_artifact_zip

# --- Upload (optional) ---
from services.upload.supabase import SupabaseUploader
from services.transform.filings_schema import clean_rows, ALLOWED_COLUMNS, REQUIRED_COLUMNS

LOG = logging.getLogger("orchestrator")


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")


def _now_wib() -> datetime:
    if JKT:
        return datetime.now(JKT)
    return datetime.now()  # fallback


def _fmt(dt: datetime, fmt: str) -> str:
    return dt.strftime(fmt)


def _safe_mkdirs(*dirs: str) -> None:
    for d in dirs:
        Path(d).mkdir(parents=True, exist_ok=True)


def _glob_many(patterns: List[str]) -> List[Path]:
    out: List[Path] = []
    for pat in patterns:
        out.extend(Path(".").glob(pat))
    return out


def pre_clean_outputs() -> None:
    targets = [
        "downloads/idx-format",
        "downloads/non-idx-format",
        "alerts_inserted",
        "alerts_not_inserted",
    ]
    for t in targets:
        p = Path(t)
        if p.exists():
            shutil.rmtree(p, ignore_errors=True)
    # file patterns
    for f in _glob_many([
        "data/*.json",
        "alerts/*.json",
        "artifacts/*.zip",
    ]):
        try:
            f.unlink()
        except Exception:
            pass
    _safe_mkdirs("downloads/idx-format", "downloads/non-idx-format", "data", "alerts", "artifacts")


def _compute_window_from_minutes(window_minutes: int) -> Tuple[str, str, str, str]:
    """
    Return (date_yyyymmdd, start_hhmm, end_hhmm, out_stub).
    Auto-handle cross-midnight via ingestion utils.
    """
    now = _now_wib()
    start = now - timedelta(minutes=window_minutes)
    date = _fmt(start, "%Y%m%d")
    sh, eh = _fmt(start, "%H:%M"), _fmt(now, "%H:%M")
    stub = f"{_fmt(start, '%Y%m%d_%H%M')}_{_fmt(now, '%Y%m%d_%H%M')}"
    return date, sh, eh, stub


def _save_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def step_check_company_map(force: bool = False) -> None:
    """
    Cek & refresh local cache company_map jika row_count berubah (otomatis).
    """
    try:
        m = get_company_map(force=force)
        LOG.info("company_map cached rows: %d", len(m or {}))
    except Exception as e:
        LOG.warning("company_map check failed (continue offline): %s", e)


def step_fetch_announcements(
    *,
    date_yyyymmdd: Optional[str],
    start_hhmm: Optional[str],
    end_hhmm: Optional[str],
    range_from: Optional[str],
    range_to: Optional[str],
    out_path: Path,
    sort_desc: bool = True,
) -> List[Dict[str, Any]]:
    """
    Ambil announcements dengan dua mode:
      - single-day + HH:MM window (minute-precision, cross-midnight ok)
      - full-day range (from..to)
    """
    data: List[Dict[str, Any]]
    if date_yyyymmdd:
        LOG.info("[FETCH] Single-day (WIB) %s %s→%s", date_yyyymmdd, start_hhmm, end_hhmm)
        data = get_ownership_announcements(
            date_yyyymmdd=date_yyyymmdd,
            start_hhmm=start_hhmm,
            end_hhmm=end_hhmm,
            logger_name="ingestion",
        )
    else:
        assert range_from and range_to
        LOG.info("[FETCH] Range (WIB) %s..%s", range_from, range_to)
        data = get_ownership_announcements_range(
            start_yyyymmdd=range_from,
            end_yyyymmdd=range_to,
            start_dt=None,
            end_dt=None,
            logger_name="ingestion",
        )

    # Sorting optional (default = newest first)
    try:
        from ingestion.utils.sorters import sort_announcements
        data = sort_announcements(data, order="desc" if sort_desc else "asc")
    except Exception:
        pass

    save_ann_json(data, out_path, logger_name="ingestion")
    return data


def step_download_pdfs(
    anns: List[Dict[str, Any]],
    *,
    out_idx_dir: Path,
    out_non_idx_dir: Path,
    meta_out: Path,
    alerts_out: Path,
    retries: int = 3,
    min_similarity: int = 80,
    dry_run: bool = False,
    verbose: bool = False,
) -> None:
    anns_model = [Announcement(**a) for a in anns]
    download_pdfs(
        announcements=anns_model,
        out_idx=str(out_idx_dir),
        out_non_idx=str(out_non_idx_dir),
        meta_out=str(meta_out),
        alerts_out=str(alerts_out),
        retries=retries,
        min_similarity=min_similarity,
        dry_run=dry_run,
        verbose=verbose,
        clean_out=False, 
    )


def step_parse_pdfs(
    *,
    idx_folder: Path,
    non_idx_folder: Path,
    idx_output: Path,
    non_idx_output: Path,
    announcements_json: Path,
) -> None:
    # IDX
    IDXClass = getattr(parser_idx_mod, "IDXParser", None)
    idx_parser = IDXClass(
        pdf_folder=str(idx_folder),
        output_file=str(idx_output),
        announcement_json=str(announcements_json),
    )
    LOG.info("[PARSER] IDXParser = %s", idx_parser.__class__.__name__)
    idx_parser.parse_folder()

  
    NonIDXClass = getattr(parser_non_idx_mod, "NonIDXParser", None)
    nonidx_parser = NonIDXClass(
        pdf_folder=str(non_idx_folder),
        output_file=str(non_idx_output),
        announcement_json=str(announcements_json),
    )
    LOG.info("[PARSER] NonIDXParser = %s", nonidx_parser.__class__.__name__)
    nonidx_parser.parse_folder()


def step_generate_filings(
    *,
    idx_parsed: Path,
    non_idx_parsed: Path,
    downloads_meta: Path,
    filings_out: Path,
    alerts_out: Path,
) -> int:
    cnt = run_generate(
        parsed_files=[str(non_idx_parsed), str(idx_parsed)],
        downloads_file=str(downloads_meta),
        output_file=str(filings_out),
        alerts_file=str(alerts_out),
    )
    LOG.info("[GENERATE] filings count = %d", cnt)
    return cnt


def step_bucketize_alerts(
    *,
    from_dir: Path = Path("alerts"),
    inserted_dir: Path = Path("alerts_inserted"),
    not_inserted_dir: Path = Path("alerts_not_inserted"),
) -> None:
    stats = bucketize_alerts(from_dir=from_dir, inserted_dir=inserted_dir, not_inserted_dir=not_inserted_dir)
    LOG.info("[BUCKETIZE] inserted=%d not_inserted=%d", stats["inserted"], stats["not_inserted"])


def step_zip_artifacts(
    *,
    prefix: str = "filings",
    include_pdfs: bool = False,
    artifact_dir: Path = Path("artifacts"),
) -> Path:
    includes = ["data/*.json", "alerts/*.json", "alerts_inserted/*.json", "alerts_not_inserted/*.json"]
    if include_pdfs:
        includes += ["downloads/**/*.pdf", "downloads/**/*.PDF"]
    zip_path, manifest = make_artifact_zip(
        prefix=prefix,
        patterns=includes,
        exclude_patterns=["**/__pycache__/**", "**/.DS_Store", "**/.venv/**"],
        out_dir=str(artifact_dir),
        base_dir=".",
    )
    LOG.info("[ARTIFACT] %s (%d files, %.2f MB)",
             zip_path, manifest.total_files, manifest.total_size / (1024*1024))
    return zip_path


def step_upload_supabase(
    *,
    input_json: Path,
    table: str,
    supabase_url: Optional[str],
    supabase_key: Optional[str],
    stop_on_missing: bool = False,
    strict_exit: bool = False,
    send_email: bool = False,  # reserved
) -> None:
    if not supabase_url or not supabase_key:
        LOG.warning("SUPABASE_URL/KEY missing; skip upload.")
        return

    uploader = SupabaseUploader(url=supabase_url, key=supabase_key)
    # load
    raw = json.loads(input_json.read_text(encoding="utf-8"))
    rows = raw["rows"] if isinstance(raw, dict) and "rows" in raw else (raw if isinstance(raw, list) else [])
    rows_clean = clean_rows(rows)

    # required columns check
    any_missing = False
    for i, r in enumerate(rows_clean):
        miss = [k for k in REQUIRED_COLUMNS if (r.get(k) is None or r.get(k) == "")]
        if miss:
            any_missing = True
            LOG.error("Row %d missing required fields: %s", i, ", ".join(miss))
            if stop_on_missing:
                raise SystemExit(3)
    if any_missing:
        LOG.warning("Some rows missing required fields; continuing.")

    # upload
    res = uploader.upload_records(
        table=table,
        rows=rows_clean,
        allowed_columns=ALLOWED_COLUMNS,
        normalize_keys=False,
        stop_on_first_error=False,
    )
    LOG.info("[UPLOAD] inserted=%d failed=%d", res.inserted, len(res.failed_rows))
    if strict_exit and res.failed_rows:
        raise SystemExit(4)


# CLI
def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="IDX filings pipeline orchestrator")

    # Run controls
    p.add_argument("--clean", action="store_true", help="Pre-clean outputs before running")
    p.add_argument("-v", "--verbose", action="store_true")

    # Fetch window modes
    g = p.add_mutually_exclusive_group()
    g.add_argument("--window-minutes", type=int, default=120,
                   help="Look back N minutes from now (WIB). Default: 120")
    g.add_argument("--window-hours", type=int, default=None,
                   help="Alternative to window-minutes; hours back from now (WIB)")

    # Explicit date mode (single day, minute precision)
    p.add_argument("--date", default=None, help="YYYYMMDD (WIB). If set, --start-hhmm and --end-hhmm required.")
    p.add_argument("--start-hhmm", default=None, help="HH:MM WIB (with --date)")
    p.add_argument("--end-hhmm", default=None, help="HH:MM WIB (with --date)")

    # Full-day range mode
    p.add_argument("--from-date", dest="from_date", default=None, help="YYYYMMDD (WIB)")
    p.add_argument("--to-date", default=None, help="YYYYMMDD (WIB)")

    # Downloader
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--min-similarity", type=int, default=80)
    p.add_argument("--dry-run-download", action="store_true")

    # Parser scope
    p.add_argument("--parser", choices=["idx", "non-idx", "both"], default="both")

    # Upload
    p.add_argument("--upload", action="store_true", help="Upload filings to Supabase (requires URL/KEY)")
    p.add_argument("--table", default="idx_filings")
    p.add_argument("--supabase-url", default=os.getenv("SUPABASE_URL"))
    p.add_argument("--supabase-key", default=os.getenv("SUPABASE_KEY"))
    p.add_argument("--stop-on-missing", action="store_true")
    p.add_argument("--strict-exit", action="store_true")

    # Artifacts
    p.add_argument("--zip-artifacts", action="store_true")
    p.add_argument("--artifact-prefix", default="filings")
    p.add_argument("--artifact-dir", default="artifacts")
    p.add_argument("--artifact-with-pdfs", action="store_true")

    return p


def main():
    ap = build_argparser()
    args = ap.parse_args()
    _setup_logging(args.verbose)

    if args.clean:
        LOG.info("[CLEAN] removing generated outputs")
        pre_clean_outputs()

    # 0) company map check/refresh (auto no-op if unchanged)
    step_check_company_map(force=False)

    # 1) Decide fetch window
    ann_out: Path
    anns: List[Dict[str, Any]]

    if args.date:
        if not (args.start_hhmm and args.end_hhmm):
            ap.error("--start-hhmm and --end-hhmm are required with --date")
        stub = f"{args.date}_{re.sub(r'[:]', '', args.start_hhmm)}_{re.sub(r'[:]', '', args.end_hhmm)}"
        ann_out = Path(f"data/idx_ann_{stub}.json")
        anns = step_fetch_announcements(
            date_yyyymmdd=args.date,
            start_hhmm=args.start_hhmm,
            end_hhmm=args.end_hhmm,
            range_from=None,
            range_to=None,
            out_path=ann_out,
        )
    elif args.from_date or args.to_date:
        if not (args.from_date and args.to_date):
            ap.error("--from-date and --to-date must be provided together")
        stub = f"{args.from_date}_{args.to_date}"
        ann_out = Path(f"data/idx_ann_{stub}.json")
        anns = step_fetch_announcements(
            date_yyyymmdd=None,
            start_hhmm=None,
            end_hhmm=None,
            range_from=args.from_date,
            range_to=args.to_date,
            out_path=ann_out,
        )
    else:
        minutes = args.window_minutes if args.window_minutes is not None else (args.window_hours or 2) * 60
        date, sh, eh, stub = _compute_window_from_minutes(minutes)
        ann_out = Path(f"data/idx_ann_{stub}.json")
        anns = step_fetch_announcements(
            date_yyyymmdd=date,
            start_hhmm=sh,
            end_hhmm=eh,
            range_from=None,
            range_to=None,
            out_path=ann_out,
        )

    # 2) Download PDFs
    step_download_pdfs(
        anns,
        out_idx_dir=Path("downloads/idx-format"),
        out_non_idx_dir=Path("downloads/non-idx-format"),
        meta_out=Path("data/downloaded_pdfs.json"),
        alerts_out=Path("alerts/low_title_similarity_alerts.json"),
        retries=args.retries,
        min_similarity=args.min_similarity,
        dry_run=args.dry_run_download,
        verbose=args.verbose,
    )

    # 3) Parse PDFs
    idx_out = Path("data/parsed_idx_output.json")
    non_idx_out = Path("data/parsed_non_idx_output.json")
    if args.parser in ("idx", "both"):
        pass  # run both anyway for simplicity; parsers will skip if folder empty
    step_parse_pdfs(
        idx_folder=Path("downloads/idx-format"),
        non_idx_folder=Path("downloads/non-idx-format"),
        idx_output=idx_out,
        non_idx_output=non_idx_out,
        announcements_json=ann_out,
    )

    # 4) Generate filings
    filings_out = Path("data/filings_data.json")
    suspicious_out = Path("alerts/suspicious_alerts.json")
    step_generate_filings(
        idx_parsed=idx_out,
        non_idx_parsed=non_idx_out,
        downloads_meta=Path("data/downloaded_pdfs.json"),
        filings_out=filings_out,
        alerts_out=suspicious_out,
    )

    # 5) Bucketize alerts → alerts_inserted / alerts_not_inserted
    step_bucketize_alerts()

    # 6) Artifacts (zip)
    if args.zip_artifacts:
        step_zip_artifacts(
            prefix=args.artifact_prefix,
            include_pdfs=args.artifact_with_pdfs,
            artifact_dir=Path(args.artifact_dir),
        )

    # 7) Optional upload to Supabase
    if args.upload:
        step_upload_supabase(
            input_json=filings_out,
            table=args.table,
            supabase_url=args.supabase_url,
            supabase_key=args.supabase_key,
            stop_on_missing=args.stop_on_missing,
            strict_exit=args.strict_exit,
        )

    LOG.info("[DONE]")


if __name__ == "__main__":
    main()
