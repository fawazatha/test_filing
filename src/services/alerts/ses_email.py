from __future__ import annotations

import os
import logging
import mimetypes
from typing import Iterable, List, Optional, Sequence, Union

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders

logger = logging.getLogger(__name__)

# Defaults via env
DEFAULT_AWS_REGION = os.getenv("AWS_REGION") or os.getenv("SES_REGION") or "ap-southeast-1"
DEFAULT_FROM_EMAIL = os.getenv("SES_FROM_EMAIL")  

SES_RAW_EMAIL_SIZE_LIMIT_BYTES = 10 * 1024 * 1024


def _ensure_list(value: Union[str, Iterable[str], None]) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        # support comma-separated string in env
        parts = [p.strip() for p in value.split(",") if p.strip()]
        return parts if parts else ([value] if value else [])
    return [v for v in value if v]


def _guess_mime_type(path: str) -> str:
    mtype, _ = mimetypes.guess_type(path)
    return mtype or "application/octet-stream"


def _build_message(
    subject: str,
    from_email: str,
    to: Sequence[str],
    body_text: str,
    body_html: Optional[str],
    cc: Sequence[str],
    bcc: Sequence[str],
    files: Sequence[str],
    charset: str = "utf-8",
) -> MIMEMultipart:
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = ", ".join(to) if to else ""
    if cc:
        msg["Cc"] = ", ".join(cc)

    alt = MIMEMultipart("alternative")

    if body_text:
        alt.attach(MIMEText(body_text, "plain", charset))
    if body_html:
        alt.attach(MIMEText(body_html, "html", charset))

    msg.attach(alt)

    for fp in files or []:
        try:
            with open(fp, "rb") as f:
                data = f.read()
            main, sub = (_guess_mime_type(fp).split("/", 1) + ["octet-stream"])[:2]
            part = MIMEBase(main, sub)
            part.set_payload(data)
            encoders.encode_base64(part)
            filename = os.path.basename(fp)
            part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
            msg.attach(part)
        except Exception as e:
            logger.error("Failed attaching file %s: %s", fp, e, exc_info=True)

    return msg


def _msg_size_estimate(msg: MIMEMultipart) -> int:
    return len(msg.as_bytes())


def send_attachments(
    to: Union[str, Iterable[str]],
    subject: str,
    body_text: str,
    files: Sequence[str],
    *,
    from_email: Optional[str] = None,
    body_html: Optional[str] = None,
    cc: Union[str, Iterable[str], None] = None,
    bcc: Union[str, Iterable[str], None] = None,
    reply_to: Union[str, Iterable[str], None] = None,
    aws_region: Optional[str] = None,
    charset: str = "utf-8",
) -> dict:

    to_list = _ensure_list(to)
    cc_list = _ensure_list(cc)
    bcc_list = _ensure_list(bcc)
    reply_to_list = _ensure_list(reply_to)
    region = aws_region or DEFAULT_AWS_REGION
    source = from_email or DEFAULT_FROM_EMAIL

    if not source:
        msg = "SES_FROM_EMAIL not configured; please set env SES_FROM_EMAIL (verified in SES)."
        logger.error(msg)
        return {"ok": False, "message_id": None, "error": msg}

    if not to_list and not cc_list and not bcc_list:
        msg = "No recipients provided (to/cc/bcc are empty)."
        logger.error(msg)
        return {"ok": False, "message_id": None, "error": msg}

    mime = _build_message(
        subject=subject,
        from_email=source,
        to=to_list,
        body_text=body_text or "",
        body_html=body_html,
        cc=cc_list,
        bcc=bcc_list,
        files=files or [],
        charset=charset,
    )

    size_bytes = _msg_size_estimate(mime)
    if size_bytes >= SES_RAW_EMAIL_SIZE_LIMIT_BYTES:
        msg = f"Email size {size_bytes} exceeds SES raw message 10MB limit."
        logger.error(msg)
        return {"ok": False, "message_id": None, "error": msg}

    try:
        ses = boto3.client("ses", region_name=region)
        response = ses.send_raw_email(
            Source=source,
            Destinations=list({*to_list, *cc_list, *bcc_list}),
            RawMessage={"Data": mime.as_string().encode(charset)},
            ReplyToAddresses=reply_to_list or None,
        )
        message_id = response.get("MessageId")
        logger.info("SES message sent. MessageId=%s", message_id)
        return {"ok": True, "message_id": message_id, "error": None}
    except (BotoCoreError, ClientError) as e:
        logger.error("SES send failed: %s", e, exc_info=True)
        return {"ok": False, "message_id": None, "error": str(e)}
