from twilio.rest import Client 
from twilio.base.exceptions import TwilioRestException 
from typing import TypedDict, Optional

from src.services.whatsapp.whatsapp_formatter import format_payload
from src.services.whatsapp.utils.config import (
    ACCOUNT_SID, TWILIO_FROM_NUMBER,
    AUTH_TOKEN, TEMPLATE_SID,
    LOGGER
)

import json 
import time
import random


class MessageResult(TypedDict):
    success: bool
    sid: Optional[str]
    error: Optional[str]


class WhatsAppSummary(TypedDict):
    sent: int
    failed: int
    errors: list[str]


def get_data_report(filings_json_path: str) -> dict[str, str]:
    try:
        with open(filings_json_path, 'r', encoding='utf-8') as file:
            data_filings = json.load(file) 
        return data_filings
    except FileNotFoundError:
        LOGGER.error(f"File not found: {filings_json_path}")
        return {}
    

def mask_number(number: str) -> str:
    if len(number) > 4:
        return f"...{number[-4:]}"
    return "****"


def send_whatsapp_message(body_content: json, to_number: str) -> MessageResult:
    if not all([
        ACCOUNT_SID, AUTH_TOKEN, 
        TEMPLATE_SID, TWILIO_FROM_NUMBER, 
        body_content, to_number
    ]):
        LOGGER.warning("Twilio credentials or recipient number are missing.")
        return

    try:
        client = Client(ACCOUNT_SID, AUTH_TOKEN)
        message = client.messages.create(
            from_=f'whatsapp:{TWILIO_FROM_NUMBER}',
            to=f'whatsapp:{to_number}',
            content_sid=TEMPLATE_SID,
            content_variables=body_content,
            
        )
        LOGGER.info(f"WhatsApp message sent successfully to {mask_number(to_number)} (SID: {message.sid})")
        return {"success": True, "sid": message.sid, "error": None}
    
    except TwilioRestException as tre:
        LOGGER.error(f"Twilio error: {tre.code} - {tre.msg}")  
        LOGGER.error(f"More details: {tre}") 
        return {"success": False, "sid": None, "error": f"Twilio error: {tre.code} - {tre.msg}"}
    
    except Exception as error:
        LOGGER.error(f"Failed to send WhatsApp message: {error}")
        return {"success": False, "sid": None, "error": str(error)}


def run_send_whatsapp(filings_data_path: str, to_number: str) -> WhatsAppSummary:
    summary: WhatsAppSummary = {"sent": 0, "failed": 0, "errors": []}

    try:
        data_report = get_data_report(filings_data_path) 
        if not data_report:
            LOGGER.warning("No data report found. Exiting message sending process.")
            return

        total_companies = data_report.get('total_companies', 0)
        total_filings = data_report.get('total_filings', 0)
        start_window = data_report.get('window_start') 
        end_window = data_report.get('window_end')

        for company in data_report.get('companies', []):
            for filing in company.get('filings', []):
                try:
                    formatted_payload = format_payload(
                        filing=filing,
                        total_companies=total_companies,
                        total_filings=total_filings,
                        window_start=start_window,
                        window_end=end_window
                    )

                    if not formatted_payload:
                        msg = f"Skipping filing {filing.get('id', 'unknown')} due to payload formatting error."
                        LOGGER.warning(msg)
                        summary["failed"] += 1
                        summary["errors"].append(msg)
                        continue

                    result = send_whatsapp_message(formatted_payload, to_number)

                    if result["success"]:
                        summary["sent"] += 1
                    else:
                        summary["failed"] += 1
                        summary["errors"].append(result["error"] or "Unknown error")

                    time.sleep(random.uniform(1, 3))

                except Exception as error:
                    LOGGER.error(f"Failed to send WhatsApp message for filing {filing.get('id', 'unknown')}: Error: {error}", 
                                 exc_info=True)
                    summary["failed"] += 1
                    summary["errors"].append(msg)

    except Exception as error:
        LOGGER.error(f"Unexpected error in run_send_whatsapp_message: Error: {error}", exc_info=True)
        summary["errors"].append(msg)

    return summary


if __name__ == '__main__':
    # test usage 
    filings_path = 'data/report/insider_report.json'
    to_number = '+62...'
    summary = run_send_whatsapp(filings_path, to_number)