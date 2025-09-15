import logging
from dotenv import load_dotenv
import os

# Load .env for local runs
load_dotenv()
logging.basicConfig(level=logging.INFO)

from src.services.alerts.ses_email import send_attachments

if __name__ == "__main__":
    # Prefer recipient from env; fall back to SES simulator (works in sandbox)
    to_email = os.getenv("TEST_TO_EMAIL", "success@simulator.amazonses.com")

    # Force region from env if you want (helps avoid cross-region confusion)
    aws_region = os.getenv("AWS_REGION") 

    res = send_attachments(
        to=to_email,
        subject="[TEST] Amazon SES raw email",
        body_text="Hello! This is a plain-text test via Amazon SES.",
        body_html="<p>Hello! This is an <b>HTML</b> test via Amazon SES.</p>",
        files=[],  # add a small file path to test attachments
        reply_to=os.getenv("TEST_REPLY_TO"),  # optional
        aws_region=aws_region,
    )
    print(res)
