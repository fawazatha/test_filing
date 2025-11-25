# src/generate/filings/cli.py
from __future__ import annotations
import argparse
import logging
import json
import sys
import os
from pathlib import Path
from typing import List, Dict, Any, Tuple
from datetime import datetime
from collections import Counter

# Core Imports
from src.core.types import FilingRecord
from src.core.transformer import transform_many

# Local & Service Imports
from src.generate.filings.utils.config import LOG_LEVEL
from src.generate.filings.runner import run as run_generate # This is the pipeline.py runner
from src.services.upload.paths import data_file, list_alert_files
from src.services.upload.supabase import SupabaseUploader, UploadResult
from src.services.upload.dedup import upload_filings_with_dedup
from services.email.manager import AlertManager
from services.email.ses_email import send_attachments

# This list now defines the *exact* columns to be uploaded.
ALLOWED_DB_COLUMNS: List[str] = [
    "symbol", "timestamp", "transaction_type", "holder_name",
    "holding_before", "holding_after", "amount_transaction",
    "share_percentage_before", "share_percentage_after", "share_percentage_transaction",
    "price", "transaction_value", "price_transaction", "title", "body",
    "purpose_of_transaction", "tags", "sector", "sub_sector", "source", "holder_type",
]

# These are the minimum fields a record must have to be valid.
REQUIRED_COLUMNS: List[str] = [
    "symbol",
    "timestamp",
    "holder_name",
    "transaction_type",
]


# Utils
def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else getattr(logging, LOG_LEVEL, logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")


def _load_env_file() -> None:
    """Load .env if it exists (optional)."""
    try:
        from dotenv import load_dotenv, find_dotenv
        env_path = find_dotenv(usecwd=True)
        if env_path:
            load_dotenv(env_path, override=False)
            logging.info("Loaded .env from %s", env_path)
        else:
            logging.debug(".env not found via find_dotenv()")
    except Exception as e:
        logging.debug("dotenv not used: %s", e)


def _parse_recipients(raw: str | None) -> List[str]:
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def _resolve_recipients_and_cfg(args):
    """
    Combine CLI flags and ENV vars for recipients & SES config.
    Priority: flags > ENV > default.
    """
    to = _parse_recipients(getattr(args, "to", None)) or _parse_recipients(os.getenv("ALERTS_TO"))
    cc = _parse_recipients(getattr(args, "cc", None)) or _parse_recipients(os.getenv("ALERTS_CC"))
    bcc = _parse_recipients(getattr(args, "bcc", None)) or _parse_recipients(os.getenv("ALERTS_BCC"))
    from_email = getattr(args, "from_email", None) or os.getenv("SES_FROM_EMAIL")
    aws_region = getattr(args, "aws_region", None) or os.getenv("AWS_REGION") or os.getenv("SES_REGION")

    return {
        "to": to,
        "cc": cc,
        "bcc": bcc,
        "from_email": from_email,
        "aws_region": aws_region,
    }


def _has_actionable(rows: list[dict]) -> bool:
    for r in rows:
        if (r.get("needs_review") is True) or (r.get("reasons") and len(r["reasons"]) > 0):
            return True
    return False


def _compose_body(bucket: str, rows: list[dict]) -> str:
    """
    Build a concise human-friendly body for alert emails so operators
    can triage without opening attachments.
    """
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    total = len(rows)

    # Count severities and top codes to give a quick pulse of what's inside.
    sev_counts = Counter((r.get("severity") or "unknown").lower() for r in rows)
    code_counts = Counter((r.get("code") or "unknown") for r in rows)

    sev_order = ["fatal", "hard", "warning", "soft", "info", "unknown"]
    sev_line_parts = [f"{s}:{sev_counts[s]}" for s in sev_order if sev_counts.get(s)]
    sev_line = ", ".join(sev_line_parts) if sev_line_parts else "none"

    top_codes = ", ".join(f"{code} ({cnt})" for code, cnt in code_counts.most_common(5))
    if not top_codes:
        top_codes = "n/a"

    lines = [
        f"Auto alert summary ({bucket.replace('_', ' ')}), aligned to the IDX filings action (~every 2 hours).",
        f"Generated: {now_str}",
        f"Total alerts: {total}",
        f"By severity: {sev_line}",
        f"Top codes: {top_codes}",
        "Attachments include full JSON payloads for detailed review.",
    ]
    return "\n".join(lines)


def _compose_body_combined(rows_in_db: list[dict], rows_not_inserted: list[dict]) -> str:
    """
    Build a structured body that covers both buckets in one email.
    """
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    def _sev_line(rows: list[dict]) -> str:
        sev_counts = Counter((r.get("severity") or "unknown").lower() for r in rows)
        sev_order = ["fatal", "hard", "warning", "soft", "info", "unknown"]
        parts = [f"{s}:{sev_counts[s]}" for s in sev_order if sev_counts.get(s)]
        return ", ".join(parts) if parts else "none"

    def _top_codes(rows: list[dict]) -> str:
        c = Counter((r.get("code") or "unknown") for r in rows)
        if not c:
            return "n/a"
        return ", ".join(f"{code} ({cnt})" for code, cnt in c.most_common(5))

    lines = [
        "Auto alert summary (combined Inserted + Not Inserted), aligned to the IDX filings action (~every 2 hours).",
        f"Generated: {now_str}",
        f"Inserted (in_db): {len(rows_in_db)} | By severity: {_sev_line(rows_in_db)} | Top codes: {_top_codes(rows_in_db)}",
        f"Not Inserted: {len(rows_not_inserted)} | By severity: {_sev_line(rows_not_inserted)} | Top codes: {_top_codes(rows_not_inserted)}",
        "Attachments include full JSON payloads for detailed review.",
    ]
    return "\n".join(lines)


def _load_bucket_rows(bucket: str) -> Tuple[list[dict], list[Path]]:
    """
    Load alert rows and the source files for a bucket.
    """
    files = list_alert_files("filings", bucket)
    files = [p for p in files if p.exists()]

    def _extract_rows_any(payload: Any) -> list[dict]:
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if isinstance(payload, dict):
            for k in ("alerts", "rows", "data", "items", "results"):
                v = payload.get(k)
                if isinstance(v, list):
                    return [x for x in v if isinstance(x, dict)]
            return [payload]
        return []

    all_rows: list[dict] = []
    for p in sorted(files):
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
            all_rows.extend(_extract_rows_any(payload))
        except Exception as e:
            logging.warning("Skip unreadable alert file %s: %s", p, e)

    return all_rows, files



def _send_bucket(bucket: str,
                 subject: str,
                 *,
                 body_text: str | None,
                 to: List[str],
                 cc: List[str],
                 bcc: List[str],
                 from_email: str | None,
                 aws_region: str | None) -> bool:
    """
    Send an SES email for a single alert bucket.
    """
    all_rows, files = _load_bucket_rows(bucket)
    if not files:
        logging.info("No %s alerts to send.", bucket)
        return False

    # Minimal guard for Inserted bucket
    if bucket == "in_db":
        if not all_rows:
            logging.info("Skip Inserted email: no rows found after parsing.")
            return False
        if not _has_actionable(all_rows):
            logging.info("Skip Inserted email: no actionable rows (no reasons and no needs_review).")
            return False

    # Compose richer body text if none provided by caller.
    computed_body = body_text or _compose_body(bucket, all_rows)

    # SES expects file paths as strings
    file_paths = [str(p) for p in files]
    resp = send_attachments(
        to=to,
        subject=subject,
        body_text=computed_body,
        files=file_paths,
        cc=cc,
        bcc=bcc,
        from_email=from_email,
        aws_region=aws_region,
    )
    if resp.get("ok"):
        logging.info("Alert email for bucket '%s' sent. MessageId=%s", bucket, resp.get("message_id"))
        return True
    else:
        logging.error("Failed to send alert email for bucket '%s': %s", bucket, resp.get("error"))
        return False


def _load_json(path: Path) -> List[Dict[str, Any]]:
    """Loads raw JSON from the pipeline output."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logging.error("Failed to read input JSON: %s", e)
        raise

    if isinstance(raw, dict):
        for key in ("rows", "data", "items"):
            if key in raw and isinstance(raw[key], list):
                return raw[key]
        logging.warning("Input JSON is an object; trying to coerce to list if possible.")
        return [raw]
    elif isinstance(raw, list):
        return raw
    else:
        logging.error("Unsupported JSON root type: %s", type(raw))
        raise ValueError("Unsupported JSON format")


def _record_alerts(am: AlertManager, res: UploadResult, table: str) -> None:
    """Record upload successes and failures to the AlertManager."""
    # success
    for _ in range(res.inserted):
        am.record({"message": f"Inserted to DB ({table})"}, inserted=True)
    # failure
    for row, err in zip(res.failed_rows, res.errors):
        am.record(
            {
                "message": "Failed to insert",
                "title": row.get("title"),
                "symbol": row.get("symbol"),
                "error": err,
            },
            inserted=False,
        )


def _first_failure_debug(uploader: SupabaseUploader, table: str, res: UploadResult) -> None:
    """Run a debug probe on the first row that failed to insert."""
    if not res.failed_rows:
        return
    sample = res.failed_rows[0]
    try:
        msg = uploader.debug_probe(table, sample)
        logging.warning("First failure debug probe: %s", msg)
    except Exception as e:
        logging.error("debug_probe failed: %s", e)


# Command Functions
def _cmd_run(args) -> None:
    """Runs the data generation pipeline (load -> transform -> process -> save)."""
    cnt = run_generate(
        parsed_files=[args.non_idx, args.idx],
        downloads_file=args.downloads,
        output_file=args.out,
        ingestion_file=args.ingestion_file, # Added this argument
        alerts_file=args.alerts,
    )
    print(f"[SUCCESS] Generated {cnt} filings -> {args.out}")


def _load_and_transform_data(input_path: Path) -> List[FilingRecord]:
    """Loads raw JSON and transforms it into clean FilingRecord objects."""
    # 1) Load raw JSON
    rows_raw: List[Dict[str, Any]] = _load_json(input_path)
    logging.info("Loaded %d raw rows from %s", len(rows_raw), input_path)

    # 2) Clean & normalize using the new central transformer
    # We pass an empty ingestion_map because 'upload' assumes data is *already*
    # processed by 'run', which *did* use the ingestion_map.
    records = transform_many(rows_raw, ingestion_map={})
    logging.info("Transformed %d records (standardized format).", len(records))
    
    return records


def _validate_records(records: List[FilingRecord], stop_on_missing: bool) -> bool:
    """Validates records against REQUIRED_COLUMNS."""
    missing_any = False
    for i, r in enumerate(records):
        missing = [k for k in REQUIRED_COLUMNS if not getattr(r, k, None)]
        if missing:
            missing_any = True
            logging.error("Record %d missing required fields: %s", i, ", ".join(missing))
            if stop_on_missing:
                sys.exit(3)
                
    if missing_any:
        logging.warning("Some records are missing required fields; continuing (stop_on_missing=off).")
        
    return not missing_any # True if valid


def _cmd_upload(args) -> None:
    """Transforms, validates, and uploads filings to Supabase with deduplication."""
    # 0) Init uploader
    try:
        uploader = SupabaseUploader(url=args.supabase_url, key=args.supabase_key)
    except RuntimeError as e:
        logging.error(str(e))
        logging.error(
            "Set ENV in .env or shell (SUPABASE_URL, SUPABASE_KEY), "
            "or use flags --supabase-url / --supabase-key."
        )
        sys.exit(2)

    table = args.table or (args.env_table or "idx_filings")
    input_path = Path(args.input)
    if not input_path.exists():
        input_path = data_file("filings") # Assumes this resolves correctly

    # 1) Load and Transform
    records = _load_and_transform_data(input_path)
    
    # 2) Validate
    if not _validate_records(records, args.stop_on_missing):
        logging.error("Validation failed. Exiting.")
        sys.exit(3)
    
    # 3) Upload with Deduplication
    if args.dry_run:
        logging.info("[DRY RUN] Skipping upload to Supabase.")
        logging.info("[DRY RUN] Would have attempted to upload %d records.", len(records))
        return

    # Convert FilingRecord objects to dicts for the dedup service
    rows_to_upload = [rec.to_db_dict() for rec in records]
    
    # Use the dedup service
    res, stats = upload_filings_with_dedup(
        uploader=uploader,
        table=table,
        rows=rows_to_upload,
        allowed_columns=ALLOWED_DB_COLUMNS,
        stop_on_first_error=False
    )
    
    logging.info(
        "Deduplication stats: input=%(input)d, intrarun_unique=%(intrarun_unique)d, "
        "existing_db_rows=%(existing_same_day_rows)d, to_insert=%(to_insert)d", 
        stats
    )

    # 4) Alerts + snapshot
    am = AlertManager("filings")
    _record_alerts(am, res, table)
    am.flush()
    am.rotate_snapshot(tag="upload")

    logging.info("Upload done. Inserted=%d, Failed=%d", res.inserted, len(res.failed_rows))

    # 5) Probe first failure
    if res.failed_rows:
        _first_failure_debug(uploader, table, res)

    # 6) Optional email (SES)
    if args.send_email:
        cfg = _resolve_recipients_and_cfg(args)
        sent_any = False

        if args.email_bucket == "both":
            rows_in_db, files_in_db = _load_bucket_rows("in_db")
            rows_not, files_not = _load_bucket_rows("not_inserted")
            if not rows_in_db and not rows_not:
                logging.info("Skip combined email: no alerts in either bucket.")
            else:
                body = args.body_text_inserted or _compose_body_combined(rows_in_db, rows_not)
                file_paths = [str(p) for p in (files_in_db + files_not)]
                resp = send_attachments(
                    to=cfg["to"],
                    subject=args.subject_inserted,
                    body_text=body,
                    files=file_paths,
                    cc=cfg["cc"],
                    bcc=cfg["bcc"],
                    from_email=cfg["from_email"],
                    aws_region=cfg["aws_region"],
                )
                if resp.get("ok"):
                    logging.info("Combined alert email sent. MessageId=%s", resp.get("message_id"))
                    sent_any = True
                else:
                    logging.error("Failed to send combined alert email: %s", resp.get("error"))
        else:
            if args.email_bucket == "in_db":
                ok = _send_bucket(
                    "in_db",
                    args.subject_inserted,
                    body_text=args.body_text_inserted,
                    to=cfg["to"], cc=cfg["cc"], bcc=cfg["bcc"],
                    from_email=cfg["from_email"], aws_region=cfg["aws_region"],
                )
                sent_any = sent_any or ok

            if args.email_bucket == "not_inserted":
                ok = _send_bucket(
                    "not_inserted",
                    args.subject_not_inserted,
                    body_text=args.body_text_not_inserted,
                    to=cfg["to"], cc=cfg["cc"], bcc=cfg["bcc"],
                    from_email=cfg["from_email"], aws_region=cfg["aws_region"],
                )
                sent_any = sent_any or ok

        print("[ALERT EMAIL SENT]" if sent_any else "[NO ALERT EMAIL SENT]")

    # 7) Exit code (optional strict)
    if args.strict_exit and res.failed_rows:
        sys.exit(4)


def _cmd_send_alerts(args) -> None:
    """Manually triggers the sending of existing alerts."""
    cfg = _resolve_recipients_and_cfg(args)
    sent_any = False

    if args.bucket == "both":
        rows_in_db, files_in_db = _load_bucket_rows("in_db")
        rows_not, files_not = _load_bucket_rows("not_inserted")
        if not rows_in_db and not rows_not:
            logging.info("Skip combined email: no alerts in either bucket.")
        else:
            body = args.body_text_inserted or _compose_body_combined(rows_in_db, rows_not)
            file_paths = [str(p) for p in (files_in_db + files_not)]
            resp = send_attachments(
                to=cfg["to"],
                subject=args.subject_inserted,
                body_text=body,
                files=file_paths,
                cc=cfg["cc"],
                bcc=cfg["bcc"],
                from_email=cfg["from_email"],
                aws_region=cfg["aws_region"],
            )
            if resp.get("ok"):
                logging.info("Combined alert email sent. MessageId=%s", resp.get("message_id"))
                sent_any = True
            else:
                logging.error("Failed to send combined alert email: %s", resp.get("error"))
    else:
        if args.bucket == "in_db":
            ok = _send_bucket(
                "in_db",
                args.subject_inserted,
                body_text=args.body_text_inserted,
                to=cfg["to"], cc=cfg["cc"], bcc=cfg["bcc"],
                from_email=cfg["from_email"], aws_region=cfg["aws_region"],
            )
            sent_any = sent_any or ok
        if args.bucket == "not_inserted":
            ok = _send_bucket(
                "not_inserted",
                args.subject_not_inserted,
                body_text=args.body_text_not_inserted,
                to=cfg["to"], cc=cfg["cc"], bcc=cfg["bcc"],
                from_email=cfg["from_email"], aws_region=cfg["aws_region"],
            )
            sent_any = sent_any or ok

    print("[ALERTS SENT]" if sent_any else "[NO ALERTS TO SEND]")


#--
# CLI Entrypoint
#--
def main():
    parser = argparse.ArgumentParser(description="Filings pipeline CLI")
    sub = parser.add_subparsers(dest="cmd")

    # Back-compat (no subcommand -> run)
    parser.add_argument("--idx", default="data/parsed_idx_output.json")
    parser.add_argument("--non-idx", default="data/parsed_non_idx_output.json")
    parser.add_argument("--downloads", default="data/downloaded_pdfs.json")
    parser.add_argument("--out", default="data/filings_data.json")
    parser.add_argument("--alerts", default="alerts/suspicious_alerts.json")
    parser.add_argument("--ingestion-file", default="data/ingestion.json", help="Path to ingestion file with announcement dates.")
    parser.add_argument("-v", "--verbose", action="store_true")

    # run
    p_run = sub.add_parser("run", help="Generate filings json")
    p_run.add_argument("--idx", default="data/parsed_idx_output.json")
    p_run.add_argument("--non-idx", default="data/parsed_non_idx_output.json")
    p_run.add_argument("--downloads", default="data/downloaded_pdfs.json")
    p_run.add_argument("--out", default="data/filings_data.json")
    p_run.add_argument("--alerts", default="alerts/suspicious_alerts.json")
    p_run.add_argument("--ingestion-file", default="data/ingestion.json", help="Path to ingestion file with announcement dates.")
    p_run.add_argument("-v", "--verbose", action="store_true")
    p_run.set_defaults(func=_cmd_run)

    # upload
    p_up = sub.add_parser("upload", help="Upload generated filings to Supabase (and optionally email alerts via SES)")
    p_up.add_argument("--input", default=str(data_file("filings")))
    p_up.add_argument("--table", default=None)
    p_up.add_argument("--env-table", default="idx_filings")
    p_up.add_argument("--send-email", action="store_true", help="Send SES email after upload")
    p_up.add_argument("--email-bucket", choices=["in_db", "not_inserted", "both"], default="both")
    p_up.add_argument("--subject-inserted", default="[FILINGS] Alerts (Inserted)")
    p_up.add_argument("--subject-not-inserted", default="[FILINGS] Alerts (Not Inserted)")
    p_up.add_argument("--body-text-inserted", default=None, help="Custom body text for Inserted email")
    p_up.add_argument("--body-text-not-inserted", default=None, help="Custom body text for Not Inserted email")
    p_up.add_argument("--to", default=None, help="Comma-separated recipients (overrides ALERTS_TO)")
    p_up.add_argument("--cc", default=None, help="Comma-separated CC (overrides ALERTS_CC)")
    p_up.add_argument("--bcc", default=None, help="Comma-separated BCC (overrides ALERTS_BCC)")
    p_up.add_argument("--from-email", default=None, help="Override SES_FROM_EMAIL")
    p_up.add_argument("--aws-region", default=None, help="Override AWS_REGION/SES_REGION")
    p_up.add_argument("--dry-run", action="store_true", help="Do not post to Supabase")
    p_up.add_argument("--stop-on-missing", action="store_true", help="Fail if required fields are missing")
    p_up.add_argument("--strict-exit", action="store_true", help="Exit 4 if any row failed to insert")
    p_up.add_argument("--supabase-url", default=None, help="Override SUPABASE_URL (optional)")
    p_up.add_argument("--supabase-key", default=None, help="Override SUPABASE_KEY (optional)")
    p_up.add_argument("-v", "--verbose", action="store_true")
    p_up.set_defaults(func=_cmd_upload)

    # send-alerts
    p_alert = sub.add_parser("send-alerts", help="Send alert emails per bucket via SES")
    p_alert.add_argument("--bucket", choices=["in_db", "not_inserted", "both"], default="both")
    p_alert.add_argument("--subject-inserted", default="[FILINGS] Alerts (Inserted)")
    p_alert.add_argument("--subject-not-inserted", default="[FILINGS] Alerts (Not Inserted)")
    p_alert.add_argument("--body-text-inserted", default=None, help="Custom body text for Inserted email")
    p_alert.add_argument("--body-text-not-inserted", default=None, help="Custom body text for Not Inserted email")
    p_alert.add_argument("--to", default=None, help="Comma-separated recipients (overrides ALERTS_TO)")
    p_alert.add_argument("--cc", default=None, help="Comma-separated CC (overrides ALERTS_CC)")
    p_alert.add_argument("--bcc", default=None, help="Comma-separated BCC (overrides ALERTS_BCC)")
    p_alert.add_argument("--from-email", default=None, help="Override SES_FROM_EMAIL")
    p_alert.add_argument("--aws-region", default=None, help="Override AWS_REGION/SES_REGION")
    p_alert.add_argument("-v", "--verbose", action="store_true")
    p_alert.set_defaults(func=_cmd_send_alerts)

    args = parser.parse_args()
    _load_env_file()
    _setup_logging(args.verbose)
    if args.cmd is None:
        _cmd_run(args)
    else:
        args.func(args)

if __name__ == "__main__":
    main()
