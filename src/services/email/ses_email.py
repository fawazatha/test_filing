from __future__ import annotations

import os
import re
import logging
import mimetypes
from typing import Iterable, List, Optional, Sequence, Union

from dotenv import load_dotenv
import boto3
from botocore.exceptions import BotoCoreError, ClientError

from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from email.utils import formatdate

# Load .env (safe to call multiple times; no-op if already loaded)
load_dotenv()

logger = logging.getLogger(__name__)

SES_RAW_EMAIL_SIZE_LIMIT_BYTES = 10 * 1024 * 1024  # 10MB raw message limit


# ---------- helpers ----------
def _ensure_list(value: Union[str, Iterable[str], None]) -> List[str]:
    """Normalize str/comma-separated/iterable into a clean list of strings."""
    if value is None:
        return []
    if isinstance(value, str):
        parts = [p.strip() for p in value.split(",") if p.strip()]
        return parts if parts else ([value] if value else [])
    return [v for v in value if v]


def _guess_mime_type(path: str) -> str:
    mtype, _ = mimetypes.guess_type(path)
    return mtype or "application/octet-stream"


def _extract_address(src: str) -> str:
    """
    Extract plain email from strings like:
      'Name <email@domain.com>' -> 'email@domain.com'
      'email@domain.com'        -> 'email@domain.com'
    """
    if not src:
        return src
    m = re.search(r"<\s*([^>]+)\s*>", src)
    return (m.group(1).strip() if m else src.strip())


def _default_region() -> str:
    """Read region at runtime so .env/export after import still works."""
    return os.getenv("AWS_REGION") or os.getenv("SES_REGION") or "ap-southeast-3"


def _default_from_email() -> Optional[str]:
    """From email (must be a verified identity in the SES region)."""
    return os.getenv("SES_FROM_EMAIL")


# ---------- MIME builder ----------
def _build_message(
    subject: str,
    from_email_display: str,   # can be 'Name <email@..>' for headers
    to: Sequence[str],
    body_text: str,
    body_html: Optional[str],
    cc: Sequence[str],
    bcc: Sequence[str],        # kept for symmetry; not written to headers
    files: Sequence[str],
    charset: str = "utf-8",
    reply_to_list: Optional[Sequence[str]] = None,
) -> MIMEMultipart:
    """
    multipart/mixed
      ├─ multipart/alternative (text/plain + text/html)
      └─ attachments...
    """
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = from_email_display
    if to:
        msg["To"] = ", ".join(to)
    if cc:
        msg["Cc"] = ", ".join(cc)
    if reply_to_list:
        msg["Reply-To"] = ", ".join(reply_to_list)
    msg["Date"] = formatdate(localtime=True)

    alt = MIMEMultipart("alternative")
    has_part = False
    if body_text:
        alt.attach(MIMEText(body_text, "plain", charset))
        has_part = True
    if body_html:
        alt.attach(MIMEText(body_html, "html", charset))
        has_part = True
    if not has_part:
        # Ensure at least one body part so message isn't empty
        alt.attach(MIMEText(" ", "plain", charset))
    msg.attach(alt)

    # Attach files
    for fp in files or []:
        try:
            with open(fp, "rb") as f:
                data = f.read()
            main, sub = (_guess_mime_type(fp).split("/", 1) + ["octet-stream"])[:2]
            part = MIMEBase(main, sub)
            part.set_payload(data)
            encoders.encode_base64(part)
            filename = os.path.basename(fp)
            part.add_header("Content-Disposition", "attachment", filename=filename)
            msg.attach(part)
        except Exception as e:
            logger.error("Failed attaching file %s: %s", fp, e, exc_info=True)

    return msg


def _msg_size_estimate(msg: MIMEMultipart) -> int:
    return len(msg.as_bytes())


# ---------- main API ----------
def send_attachments(
    to: Union[str, Iterable[str]],
    subject: str,
    body_text: str,
    files: Sequence[str],
    *,
    from_email: Optional[str] = None,                 # may be 'Name <email@..>'
    body_html: Optional[str] = None,
    cc: Union[str, Iterable[str], None] = None,
    bcc: Union[str, Iterable[str], None] = None,
    reply_to: Union[str, Iterable[str], None] = None, # will be placed in MIME header
    aws_region: Optional[str] = None,
    charset: str = "utf-8",
) -> dict:
    """
    Send an email with optional HTML and attachments using Amazon SES (SendRawEmail).
    - `from_email` must be a verified identity in the SES region (or set SES_FROM_EMAIL env).
    - If SES account is in sandbox, recipients must also be verified (or use simulator).
    """
    to_list = _ensure_list(to)
    cc_list = _ensure_list(cc)
    bcc_list = _ensure_list(bcc)
    reply_to_list = _ensure_list(reply_to)

    region = aws_region or _default_region()
    source_display = from_email or _default_from_email()

    if not source_display:
        msg = "SES_FROM_EMAIL not configured; set env SES_FROM_EMAIL (verified in SES)."
        logger.error(msg)
        return {"ok": False, "message_id": None, "error": msg}

    if not to_list and not cc_list and not bcc_list:
        msg = "No recipients provided (to/cc/bcc are empty)."
        logger.error(msg)
        return {"ok": False, "message_id": None, "error": msg}

    logger.info("SES region=%s, source_header=%s, to=%s, cc=%s, bcc=%s",
                region, source_display, to_list, cc_list, bcc_list)

    mime = _build_message(
        subject=subject,
        from_email_display=source_display,  # keep display name in headers
        to=to_list,
        body_text=body_text or "",
        body_html=body_html,
        cc=cc_list,
        bcc=bcc_list,
        files=files or [],
        charset=charset,
        reply_to_list=reply_to_list,
    )

    size_bytes = _msg_size_estimate(mime)
    if size_bytes >= SES_RAW_EMAIL_SIZE_LIMIT_BYTES:
        msg = f"Email size {size_bytes} exceeds SES raw message 10MB limit."
        logger.error(msg)
        return {"ok": False, "message_id": None, "error": msg}

    try:
        ses = boto3.client("ses", region_name=region)
        response = ses.send_raw_email(
            Source=_extract_address(source_display),  # plain address for API call
            Destinations=list({*to_list, *cc_list, *bcc_list}),
            RawMessage={"Data": mime.as_bytes()},
            # Do NOT pass ReplyToAddresses here; set Reply-To in the MIME header instead.
        )
        message_id = response.get("MessageId")
        logger.info("SES message sent. MessageId=%s", message_id)
        return {"ok": True, "message_id": message_id, "error": None}
    except (BotoCoreError, ClientError) as e:
        logger.error("SES send failed: %s", e, exc_info=True)
        return {"ok": False, "message_id": None, "error": str(e)}
