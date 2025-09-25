from __future__ import annotations
import os, re, base64, logging
from typing import Iterable, List
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition

logger = logging.getLogger(__name__)

def _parse_recipients(val: str | None) -> List[str]:
    if not val: return []
    return [x.strip() for x in re.split(r"[;,]", val) if x.strip()]

def _attach(path: str) -> Attachment:
    with open(path, "rb") as f: raw = f.read()
    b64 = base64.b64encode(raw).decode("ascii")
    a = Attachment()
    a.file_content = FileContent(b64)
    a.file_type = FileType("application/json")
    a.file_name = FileName(os.path.basename(path))
    a.disposition = Disposition("attachment")
    return a

def send_attachments(domain: str, files: Iterable[str], subject: str) -> bool:
    api_key = os.getenv("SENDGRID_API_KEY")
    from_email = os.getenv("SENDGRID_FROM", "no-reply@example.com")
    to_list = _parse_recipients(os.getenv("ALERT_TO") or os.getenv("SENDGRID_TO"))
    if not api_key:
        logger.error("SENDGRID_API_KEY missing"); return False
    if not to_list:
        logger.warning("No recipients (ALERT_TO / SENDGRID_TO)"); return False

    files = list(files)
    if not files:
        logger.info("No attachments to send."); return False

    msg = Mail(from_email=from_email, to_emails=to_list, subject=subject,
               html_content=f"<p>Please see attached alerts for <b>{domain}</b>.</p>")
    for p in files:
        try: msg.add_attachment(_attach(p))
        except Exception as e: logger.error("Skip attachment %s: %s", p, e)

    try:
        sg = SendGridAPIClient(api_key); resp = sg.send(msg)
        logger.info("SendGrid response: %s", resp.status_code)
        return 200 <= resp.status_code < 300
    except Exception as e:
        logger.error("SendGrid send failed: %s", e); return False
