from __future__ import annotations

import os
import json
import glob
import argparse
import logging
from typing import Any, Dict, List, Sequence, Optional, Tuple

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# pakai renderer dari alerts_mailer agar konsisten tampilannya
from src.services.alerts.alerts_mailer import _render_email_content  # type: ignore
# kirim langsung dengan daftar attachment asli
from src.services.alerts.ses_email import send_attachments

# -------- helpers --------
def _tolist(x: Optional[Sequence[str] | str]) -> List[str]:
    if x is None:
        return []
    if isinstance(x, str):
        return [s.strip() for s in x.split(",") if s.strip()]
    return [s for s in x if s]

def _read_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _coerce_alerts(obj) -> List[Dict[str, Any]]:
    """
    Normalisasi berbagai bentuk JSON jadi list[dict]:
      - [ {...}, {...} ]
      - {"alerts":[...]} / {"data":[...]} / {"items":[...]} / {"results":[...]}
      - {"symbol": "...", ...} (single object) -> [obj]
    """
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        for k in ("alerts", "data", "items", "results"):
            if k in obj and isinstance(obj[k], list):
                return [x for x in obj[k] if isinstance(x, dict)]
        return [obj]  # single object
    return []

def _gather_dir(dir_path: str) -> List[str]:
    if not dir_path:
        return []
    return [p for p in glob.glob(os.path.join(dir_path, "*.json")) if os.path.exists(p)]

def _load_many(files: List[str]) -> List[Dict[str, Any]]:
    all_alerts: List[Dict[str, Any]] = []
    for fp in files:
        try:
            obj = _read_json(fp)
            alerts = _coerce_alerts(obj)
            logger.info("Loaded %s alerts from %s", len(alerts), fp)
            all_alerts.extend(alerts)
        except Exception as e:
            logger.warning("Failed to load %s: %s (skipped)", fp, e)
    return all_alerts

def _pick_attachments(paths: List[str], max_bytes: int) -> Tuple[List[str], int, List[str]]:
    """
    Pilih subset file agar total size <= max_bytes (buffer untuk body & headers).
    Strategi: sort asc by size, tambahkan satu per satu.
    Return: (picked_paths, total_bytes, skipped_paths)
    """
    files = []
    for p in paths:
        try:
            sz = os.path.getsize(p)
            files.append((p, sz))
        except OSError:
            logger.warning("Cannot stat %s (skipped)", p)
    files.sort(key=lambda x: x[1])  # kecil dulu

    picked, total = [], 0
    for p, sz in files:
        if total + sz <= max_bytes:
            picked.append(p); total += sz
    picked_set = set(picked)
    skipped = [p for p, _ in files if p not in picked_set]
    return picked, total, skipped

# -------- sender --------
def _send_group(
    *,
    alerts: List[Dict[str, Any]],
    source_files: List[str],
    title: str,
    to: Optional[Sequence[str] | str],
    cc: Optional[Sequence[str] | str],
    bcc: Optional[Sequence[str] | str],
    region: Optional[str],
    send_empty: bool,
    attach_limit_bytes: int,
) -> dict | None:
    # Heartbeat bila kosong
    if not alerts and send_empty:
        subject, body_text, body_html = _render_email_content([], title=f"{title} (No Alerts)")
        res = send_attachments(
            to=_tolist(to) or None,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            files=[],  # heartbeat: tanpa lampiran
            cc=_tolist(cc) or None,
            bcc=_tolist(bcc) or None,
            aws_region=region,
        )
        logger.info("Send result for '%s' (empty): %s", title, res)
        return res

    # Skip jika tidak ada alert
    if not alerts:
        logger.info("No alerts for '%s' — skipping email.", title)
        return None

    # Pilih lampiran asli tanpa melebihi limit
    picked, total_bytes, skipped = _pick_attachments(source_files, attach_limit_bytes)
    if skipped:
        logger.warning(
            "Some attachments skipped for '%s' due to size limit: %s",
            title, ", ".join(os.path.basename(s) for s in skipped)
        )

    # Render ringkasan konten alerts (HTML + plain)
    subject, body_text, body_html = _render_email_content(alerts, title=title)

    # Tambahkan daftar nama file terlampir di akhir body_text agar jelas
    if picked:
        body_text += "\n\nAttached files:\n" + "\n".join(f"- {os.path.basename(p)}" for p in picked)

    logger.info("Sending '%s' with %d alert(s), %d attachment(s), ~%d bytes",
                title, len(alerts), len(picked), total_bytes)

    res = send_attachments(
        to=_tolist(to) or None,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
        files=picked,
        cc=_tolist(cc) or None,
        bcc=_tolist(bcc) or None,
        aws_region=region,
    )
    logger.info("Send result for '%s': %s", title, res)
    return res


def main():
    ap = argparse.ArgumentParser(description="Send two alerts emails: inserted & not_inserted (attach original files).")
    ap.add_argument("--inserted-dir", default=os.getenv("ALERT_INSERTED_DIR", "alert_inserted"),
                    help="Folder JSON alerts yang SUDAH masuk DB (default: alert_inserted)")
    ap.add_argument("--not-inserted-dir", default=os.getenv("ALERT_NOT_INSERTED_DIR", "alert_not_inserted"),
                    help="Folder JSON alerts yang BELUM masuk DB (default: alert_not_inserted)")

    ap.add_argument("--to-inserted", default=os.getenv("ALERT_TO_EMAIL_INSERTED", os.getenv("ALERT_TO_EMAIL", "")),
                    help="Comma-separated recipients untuk email INSERTED")
    ap.add_argument("--to-not-inserted", default=os.getenv("ALERT_TO_EMAIL_NOT_INSERTED", os.getenv("ALERT_TO_EMAIL", "")),
                    help="Comma-separated recipients untuk email NOT_INSERTED")
    ap.add_argument("--cc-inserted", default=os.getenv("ALERT_CC_EMAIL_INSERTED", ""))
    ap.add_argument("--cc-not-inserted", default=os.getenv("ALERT_CC_EMAIL_NOT_INSERTED", ""))
    ap.add_argument("--bcc-inserted", default=os.getenv("ALERT_BCC_EMAIL_INSERTED", ""))
    ap.add_argument("--bcc-not-inserted", default=os.getenv("ALERT_BCC_EMAIL_NOT_INSERTED", ""))

    ap.add_argument("--title-inserted", default=os.getenv("ALERT_TITLE_INSERTED", "IDX Alerts — Inserted to DB (Checking Needed)"))
    ap.add_argument("--title-not-inserted", default=os.getenv("ALERT_TITLE_NOT_INSERTED", "IDX Alerts — Not Inserted (Action Needed)"))

    ap.add_argument("--region", default=os.getenv("AWS_REGION") or os.getenv("SES_REGION") or "ap-southeast-3")
    ap.add_argument("--send-empty", action="store_true", help="Tetap kirim email meski tidak ada alert (heartbeat)")
    ap.add_argument("--attach-budget", type=int, default=int(os.getenv("ATTACH_BUDGET_BYTES", "7500000")),
                    help="Batas total size untuk lampiran (default ~7.5MB untuk aman di limit 10MB raw)")
    args = ap.parse_args()

    inserted_files = _gather_dir(args.inserted_dir)
    not_inserted_files = _gather_dir(args.not_inserted_dir)

    if not inserted_files and not not_inserted_files and not args.send_empty:
        logger.error("Tidak ada file JSON di '%s' dan '%s'.", args.inserted_dir, args.not_inserted_dir)
        return

    inserted_alerts = _load_many(inserted_files)
    not_inserted_alerts = _load_many(not_inserted_files)

    to_ins = _tolist(args.to_inserted) or None
    to_not = _tolist(args.to_not_inserted) or None
    cc_ins = _tolist(args.cc_inserted) or None
    cc_not = _tolist(args.cc_not_inserted) or None
    bcc_ins = _tolist(args.bcc_inserted) or None
    bcc_not = _tolist(args.bcc_not_inserted) or None

    _send_group(
        alerts=inserted_alerts,
        source_files=inserted_files,
        title=args.title_inserted,
        to=to_ins, cc=cc_ins, bcc=bcc_ins,
        region=args.region,
        send_empty=args.send_empty,
        attach_limit_bytes=args.attach_budget,
    )

    _send_group(
        alerts=not_inserted_alerts,
        source_files=not_inserted_files,
        title=args.title_not_inserted,
        to=to_not, cc=cc_not, bcc=bcc_not,
        region=args.region,
        send_empty=args.send_empty,
        attach_limit_bytes=args.attach_budget,
    )

if __name__ == "__main__":
    main()
