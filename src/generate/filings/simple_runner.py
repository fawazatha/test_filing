from deep_translator import GoogleTranslator

from src.core.transformer import _generate_title_and_body
from src.core.types import FilingRecord, PriceTransaction
from src.generate.filings.utils.processors import process_filing_record
from src.generate.filings.utils.consolidators import dedupe_rows
from src.generate.filings.utils.loaders import build_downloads_meta_map
from src.generate.filings.utils.processors import _resolve_doc_meta
from src.services.alert.schema import build_alert
from src.generate.filings.utils.provider import get_company_info

import json
import logging


LOGGER = logging.getLogger("simple_runner: filing with new document")


def translator(text: str) -> str:
    if not text: 
        return ''
    
    try:
        translated = GoogleTranslator(source='auto', target='en').translate(text) 
        return translated
    
    except Exception as error:
        LOGGER.error(f'Error translator: {error}')
        return None


def load_json(path: str): 
    try:
        with open(path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return data
        
    except Exception as error:
        LOGGER.error(f'Error loading JSON file {path}: {error}')
        return None


def run_simple(
    parsed_files: str, 
    output_file: str, 
    alerts_file: str, 
    downloads_file: str
):
    payloads = load_json(parsed_files)
    downloads_map = build_downloads_meta_map(downloads_file)

    records = []
    for row in payloads:
        holder_name = row.get('holder_name')
        purpose = row.get('purpose', '')
        company_name = row.get('company_name')
        transaction_type = row.get('transaction_type', None)
        amount_transaction = row.get('amount_transaction')
        holding_before = row.get('holding_before')
        holding_after = row.get('holding_after')
        symbol = row.get('symbol')

        row['purpose'] = translator(row.get('purpose', ''))
        title, body = _generate_title_and_body(
            holder_name=holder_name,
            company_name=company_name,
            purpose_en=purpose,
            tx_type=transaction_type,
            amount=amount_transaction,
            holding_before=holding_before,
            holding_after=holding_after
        )
        row['title'] = title
        row['body'] = body

        # Get sector / sub-sector 
        company_info = get_company_info(symbol)
        row['sector'] = company_info.sector or ''
        row['sub_sector'] = company_info.sub_sector or ''

        rec = map_row_to_record_directly(row)
        records.append(rec)

    processed_records = []
    alerts_to_save = []
    
    for record in records:
        # Resolve metadata for links in email
        doc_meta = _resolve_doc_meta(record, downloads_map)
        
        # This attaches reasons/audit_flags to the record object
        checked_rec = process_filing_record(record, doc_meta=doc_meta)
        processed_records.append(checked_rec)

        # We look inside the record we just processed
        audit = getattr(checked_rec, "audit_flags", {}) or {}
        reasons = audit.get("reasons") or []
        
        # Only create alert if there are reasons OR it needs review
        if reasons or audit.get("needs_review"):
            
            # Prepare context for the email template
            ann_block = audit.get("announcement") or {}
            
            # Group reasons (e.g., if one row has both missing_price AND stale_price)
            # Use the "primary" code (skip_reason) as the main email title
            primary_code = getattr(checked_rec, "skip_reason", None) or "filings_audit"
            
            # Additional context for the email body
            ctx = {
                "symbol": checked_rec.symbol,
                "holder_name": checked_rec.holder_name,
                "type": checked_rec.transaction_type,
                "amount": checked_rec.amount_transaction,
                "price": checked_rec.price,
                "value": checked_rec.transaction_value,
                "skip_reason": primary_code
            }

            # Build the Alert Object (Standard Schema)
            alert = build_alert(
                category="inserted", # or "not_inserted" if you filter them out later
                stage="filings",
                code=primary_code,
                doc_filename=checked_rec.source,
                context_doc_url=ann_block.get("pdf_url") or ann_block.get("url"),
                context_doc_title=ann_block.get("title"),
                announcement=ann_block,
                reasons=reasons,
                ctx=ctx,
                needs_review=bool(audit.get("needs_review"))
            )
            # Flatten common fields for easier JSON reading
            alert.update(ctx) 
            alerts_to_save.append(alert)

    # Convert Records back to Dicts for JSON
    output_rows = [processed_record.to_db_dict() for processed_record in processed_records]
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_rows, f, indent=2, default=str)

    # Save Alerts (The Bad Data Report)
    with open(alerts_file, 'w', encoding='utf-8') as f:
        json.dump(alerts_to_save, f, indent=2, default=str)

    print(f"Processed {len(processed_records)} records.")
    print(f"Generated {len(alerts_to_save)} alerts -> Saved to {alerts_file}")
    
    return len(processed_records)


def map_row_to_record_directly(row: dict[str, any]) -> FilingRecord:
    raw_price_transactions = row.get("price_transaction", [])
    price_tx_objs = []
    
    if isinstance(raw_price_transactions, list):
        for raw_price_transaction in raw_price_transactions:
            price_tx_objs.append(PriceTransaction(
                transaction_date=raw_price_transaction.get("date"),
                transaction_type=raw_price_transaction.get("type"),
                transaction_price=raw_price_transaction.get("price"),
                transaction_share_amount=raw_price_transaction.get("amount_transacted")
            ))

    return FilingRecord(
        symbol=row.get("symbol"),
        holder_name=row.get("holder_name"),
        timestamp=row.get("timestamp"), 
        
        tags=row.get("tags", []), 
        
        # Transaction Info
        transaction_type=row.get("transaction_type"),
        amount_transaction=row.get("amount_transaction"),
        price=row.get("price"),
        transaction_value=row.get("transaction_value"),
        
        # title and body
        title=row.get('title'),
        body=row.get('body'),
        sector=row.get("sector"),
        sub_sector=row.get("sub_sector"),

        # Holdings
        holding_before=row.get("holding_before"),
        holding_after=row.get("holding_after"),
        share_percentage_before=row.get("share_percentage_before"),
        share_percentage_after=row.get("share_percentage_after"),
        
        # Metadata
        source=row.get("source"),
        # Raw purpose
        purpose_of_transaction=row.get("purpose"), 
        
        # Pass the price_transaction list if it exists
        price_transaction=price_tx_objs,
        
        raw_data=row

        # filings_input_source='automated',
    )

       

  

if __name__ == '__main__': 
    parsed_files = 'data/parsed_idx_output.json'
    output_file = 'data/simple_runner_output.json'
    alerts_file = 'data/simple_runner_alerts.json'
    downloads_file = 'data/downloaded_pdfs.json'
    run_simple(
        parsed_files=parsed_files,
        output_file=output_file,
        alerts_file=alerts_file,
        downloads_file=downloads_file
    )