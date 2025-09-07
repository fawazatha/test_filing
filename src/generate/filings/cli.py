from __future__ import annotations
import argparse
import logging
import json
import sys
import os
from pathlib import Path
from typing import List, Dict, Any

from .config import LOG_LEVEL
from .runner import run as run_generate

from services.io.paths import data_file, list_alert_files
from services.upload.supabase import SupabaseUploader
from services.alerts.manager import AlertManager
# === GANTI KE SES ===
from services.alerts.ses_email import send_attachments
from services.transform.filings_schema import (
    clean_rows, ALLOWED_COLUMNS, REQUIRED_COLUMNS
)


# -----------------------------
# Utils
# -----------------------------
def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else getattr(logging, LOG_LEVEL, logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")


def _load_env_file() -> None:
    """Load .env kalau ada (tidak wajib)."""
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
    Menggabungkan flag CLI dan ENV untuk penerima & config SES.
    Prioritas: flags > ENV > default kosong.
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


def _send_bucket(bucket: str,
                 subject: str,
                 *,
                 body_text: str,
                 to: List[str],
                 cc: List[str],
                 bcc: List[str],
                 from_email: str | None,
                 aws_region: str | None) -> bool:
    """
    Kirim email SES untuk satu bucket alert.
    """
    files = list_alert_files("filings", bucket)
    files = [p for p in files if p.exists()]
    if not files:
        logging.info("No %s alerts to send.", bucket)
        return False

    # SES expects file paths as strings
    file_paths = [str(p) for p in files]
    resp = send_attachments(
        to=to,
        subject=subject,
        body_text=body_text,
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


def _record_alerts(am: AlertManager, res, table: str) -> None:
    # sukses
    for _ in range(res.inserted):
        am.record({"message": f"Inserted to DB ({table})"}, inserted=True)
    # gagal
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


def _first_failure_debug(uploader: SupabaseUploader, table: str, res) -> None:
    if not res.failed_rows:
        return
    sample = res.failed_rows[0]
    try:
        msg = uploader.debug_probe(table, sample)
        logging.warning("First failure debug probe: %s", msg)
    except Exception as e:
        logging.error("debug_probe failed: %s", e)


# -----------------------------
# Commands
# -----------------------------
def _cmd_run(args) -> None:
    cnt = run_generate(
        parsed_files=[args.non_idx, args.idx],
        downloads_file=args.downloads,
        output_file=args.out,
        alerts_file=args.alerts,
    )
    print(f"[SUCCESS] Generated {cnt} filings -> {args.out}")


def _cmd_upload(args) -> None:
    # 0) Inisialisasi uploader (ENV sudah diload di main())
    try:
        uploader = SupabaseUploader(url=args.supabase_url, key=args.supabase_key)
    except RuntimeError as e:
        logging.error(str(e))
        logging.error(
            "Set ENV di .env atau shell (SUPABASE_URL, SUPABASE_KEY), "
            "atau pakai flags --supabase-url / --supabase-key."
        )
        sys.exit(2)

    table = args.table or (args.env_table or "idx_filings")
    input_path = Path(args.input)
    if not input_path.exists():
        input_path = data_file("filings")

    # 1) Load JSON mentah
    rows_raw: List[Dict[str, Any]] = _load_json(input_path)
    logging.info("Loaded %d raw rows from %s", len(rows_raw), input_path)

    # 2) Clean & normalize
    rows_clean = clean_rows(rows_raw)
    logging.info("Cleaned rows -> ready for upload (tickers set to NULL).")

    # Sanity sample
    if rows_clean:
        sample = rows_clean[0]
        for k in ("symbol", "sector", "tags", "tickers", "price_transaction"):
            v = sample.get(k, None)
            logging.debug("FIELD %s -> type=%s value=%r", k, type(v).__name__, v)

    # Required fields
    missing_any = False
    for i, r in enumerate(rows_clean):
        missing = [k for k in REQUIRED_COLUMNS if (r.get(k) is None or r.get(k) == "")]
        if missing:
            missing_any = True
            logging.error("Row %d missing required fields: %s", i, ", ".join(missing))
            if args.stop_on_missing:
                sys.exit(3)
    if missing_any:
        logging.warning("Some rows are missing required fields; continuing (stop_on_missing=off).")

    # 3) Upload
    if args.dry_run:
        logging.info("[DRY RUN] Skipping upload to Supabase.")
        return

    res = uploader.upload_records(
        table=table,
        rows=rows_clean,
        allowed_columns=ALLOWED_COLUMNS,
        normalize_keys=False,
        stop_on_first_error=False,
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

        if args.email_bucket in ("in_db", "both"):
            ok = _send_bucket(
                "in_db",
                args.subject_inserted,
                body_text=args.body_text_inserted or "Attached are 'Inserted' alerts.",
                to=cfg["to"], cc=cfg["cc"], bcc=cfg["bcc"],
                from_email=cfg["from_email"], aws_region=cfg["aws_region"],
            )
            sent_any = sent_any or ok

        if args.email_bucket in ("not_inserted", "both"):
            ok = _send_bucket(
                "not_inserted",
                args.subject_not_inserted,
                body_text=args.body_text_not_inserted or "Attached are 'Not Inserted' alerts.",
                to=cfg["to"], cc=cfg["cc"], bcc=cfg["bcc"],
                from_email=cfg["from_email"], aws_region=cfg["aws_region"],
            )
            sent_any = sent_any or ok

        print("[ALERT EMAIL SENT]" if sent_any else "[NO ALERT EMAIL SENT]")

    # 7) Exit code (opsional strict)
    if args.strict_exit and res.failed_rows:
        sys.exit(4)


def _cmd_send_alerts(args) -> None:
    cfg = _resolve_recipients_and_cfg(args)
    sent_any = False

    if args.bucket in ("in_db", "both"):
        ok = _send_bucket(
            "in_db",
            args.subject_inserted,
            body_text=args.body_text_inserted or "Attached are 'Inserted' alerts.",
            to=cfg["to"], cc=cfg["cc"], bcc=cfg["bcc"],
            from_email=cfg["from_email"], aws_region=cfg["aws_region"],
        )
        sent_any = sent_any or ok

    if args.bucket in ("not_inserted", "both"):
        ok = _send_bucket(
            "not_inserted",
            args.subject_not_inserted,
            body_text=args.body_text_not_inserted or "Attached are 'Not Inserted' alerts.",
            to=cfg["to"], cc=cfg["cc"], bcc=cfg["bcc"],
            from_email=cfg["from_email"], aws_region=cfg["aws_region"],
        )
        sent_any = sent_any or ok

    print("[ALERTS SENT]" if sent_any else "[NO ALERTS TO SEND]")


# -----------------------------
# CLI
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Filings pipeline CLI")
    sub = parser.add_subparsers(dest="cmd")

    # Back-compat (no subcommand -> run)
    parser.add_argument("--idx", default="data/parsed_idx_output.json")
    parser.add_argument("--non-idx", default="data/parsed_non_idx_output.json")
    parser.add_argument("--downloads", default="data/downloaded_pdfs.json")
    parser.add_argument("--out", default="data/filings_data.json")
    parser.add_argument("--alerts", default="alerts/suspicious_alerts.json")
    parser.add_argument("-v", "--verbose", action="store_true")

    # run
    p_run = sub.add_parser("run", help="Generate filings json")
    p_run.add_argument("--idx", default="data/parsed_idx_output.json")
    p_run.add_argument("--non-idx", default="data/parsed_non_idx_output.json")
    p_run.add_argument("--downloads", default="data/downloaded_pdfs.json")
    p_run.add_argument("--out", default="data/filings_data.json")
    p_run.add_argument("--alerts", default="alerts/suspicious_alerts.json")
    p_run.add_argument("-v", "--verbose", action="store_true")
    p_run.set_defaults(func=_cmd_run)

    # upload (+ optional email via SES)
    p_up = sub.add_parser("upload", help="Upload generated filings to Supabase (and optionally email alerts via SES)")
    p_up.add_argument("--input", default=str(data_file("filings")))
    p_up.add_argument("--table", default=None)
    p_up.add_argument("--env-table", default="idx_filings")
    # Email controls
    p_up.add_argument("--send-email", action="store_true", help="Send SES email after upload")
    p_up.add_argument("--email-bucket", choices=["in_db", "not_inserted", "both"], default="both")
    p_up.add_argument("--subject-inserted", default="[FILINGS] Alerts (Inserted)")
    p_up.add_argument("--subject-not-inserted", default="[FILINGS] Alerts (Not Inserted)")
    p_up.add_argument("--body-text-inserted", default=None, help="Custom body text for Inserted email")
    p_up.add_argument("--body-text-not-inserted", default=None, help="Custom body text for Not Inserted email")
    # Recipients & SES config (flags override ENV)
    p_up.add_argument("--to", default=None, help="Comma-separated recipients (overrides ALERTS_TO)")
    p_up.add_argument("--cc", default=None, help="Comma-separated CC (overrides ALERTS_CC)")
    p_up.add_argument("--bcc", default=None, help="Comma-separated BCC (overrides ALERTS_BCC)")
    p_up.add_argument("--from-email", default=None, help="Override SES_FROM_EMAIL")
    p_up.add_argument("--aws-region", default=None, help="Override AWS_REGION/SES_REGION")
    # Controls
    p_up.add_argument("--dry-run", action="store_true", help="Do not post to Supabase")
    p_up.add_argument("--stop-on-missing", action="store_true", help="Fail if required fields are missing")
    p_up.add_argument("--strict-exit", action="store_true", help="Exit 4 if any row failed to insert")
    # Supabase creds override
    p_up.add_argument("--supabase-url", default=None, help="Override SUPABASE_URL (optional)")
    p_up.add_argument("--supabase-key", default=None, help="Override SUPABASE_KEY (optional)")
    p_up.add_argument("-v", "--verbose", action="store_true")
    p_up.set_defaults(func=_cmd_upload)

    # send-alerts (manual) via SES
    p_alert = sub.add_parser("send-alerts", help="Send alert emails per bucket via SES")
    p_alert.add_argument("--bucket", choices=["in_db", "not_inserted", "both"], default="both")
    p_alert.add_argument("--subject-inserted", default="[FILINGS] Alerts (Inserted)")
    p_alert.add_argument("--subject-not-inserted", default="[FILINGS] Alerts (Not Inserted)")
    p_alert.add_argument("--body-text-inserted", default=None, help="Custom body text for Inserted email")
    p_alert.add_argument("--body-text-not-inserted", default=None, help="Custom body text for Not Inserted email")
    # Recipients & SES config (flags override ENV)
    p_alert.add_argument("--to", default=None, help="Comma-separated recipients (overrides ALERTS_TO)")
    p_alert.add_argument("--cc", default=None, help="Comma-separated CC (overrides ALERTS_CC)")
    p_alert.add_argument("--bcc", default=None, help="Comma-separated BCC (overrides ALERTS_BCC)")
    p_alert.add_argument("--from-email", default=None, help="Override SES_FROM_EMAIL")
    p_alert.add_argument("--aws-region", default=None, help="Override AWS_REGION/SES_REGION")
    p_alert.add_argument("-v", "--verbose", action="store_true")
    p_alert.set_defaults(func=_cmd_send_alerts)

    args = parser.parse_args()
    _load_env_file()                # load .env duluan
    _setup_logging(args.verbose)
    if args.cmd is None:
        _cmd_run(args)
    else:
        args.func(args)


if __name__ == "__main__":
    main()
