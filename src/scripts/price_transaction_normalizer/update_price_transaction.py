from supabase import create_client, Client 
from dotenv import load_dotenv
from datetime import datetime 
from pathlib import Path

from downloader.client import init_http, get_pdf_bytes_minimal, seed_and_retry_minimal
from src.common.files import ensure_dir, safe_filename_from_url

import os
import json 
import fitz
import requests

load_dotenv(override=True)
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY') 


def write_to_json(data: list[dict], filename: str) -> None:
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    except Exception as error:
        print(f"Error writing to JSON file: {error}")
        raise


def truncate_to_minute(timestamp_str):
    dt = datetime.fromisoformat(timestamp_str)
    return dt.replace(second=0, microsecond=0)


def check_length_old_filing(filing_payload: list[dict[str, any]]): 
    count = 0
    for filing in filing_payload:
        price_transaction = filing.get('price_transaction', []) 
        if isinstance(price_transaction, dict): 
            count += 1 
    print(f'Total filings with price_transaction as dict: {count}')
    

def create_composite_key(record: dict) -> set: 
    return ( 
        record.get('source'),
        truncate_to_minute(record.get('timestamp')),
        record.get('symbol'),
        record.get('transaction_type'),
        int(record.get('amount_transaction', 0) or 0)
    )


def create_composite_key_second_fallback(record: dict) -> set: 
    return ( 
        record.get('source'),
        # truncate_to_minute(record.get('timestamp')),
        record.get('symbol'),
        record.get('holding_before'),
        record.get('holding_after'),
    )


def create_composite_key_third_fallback(record: dict) -> set: 
    return ( 
        record.get('source'),
        # truncate_to_minute(record.get('timestamp')),
        record.get('symbol'),
        record.get('share_percentage_before'),
        record.get('share_percentage_after'),
    )


def create_composite_key_fourth_fallback(record: dict) -> set: 
    return ( 
        record.get('source'),
        #record.get('symbol'),
        # record.get('holder_name'),
        record.get('holding_before'),
        record.get('holding_after'),
        record.get('share_percentage_before'),
        record.get('share_percentage_after'),
    )


def create_composite_key_fifth_fallback(record: dict) -> set: 
    return ( 
        record.get('source'),
        record.get('title'),
        record.get('share_percentage_before'),
        record.get('share_percentage_after'),
    )


def get_supabase_client() -> Client:
    try: 
        supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
        return supabase_client
    except Exception as error:
        print(f"Error creating Supabase client: {error}")
        raise


def get_idx_filing_data(supabase_client: Client, table_name: str, is_filing_old: bool = True):
    try: 
        response = (
            supabase_client. 
            table(table_name). 
            select('*').
            execute()
        )

        final_filing_output = []
        if response.data: 
            for record in response.data:
                timestamp_object = datetime.fromisoformat(record.get('timestamp'))
                if timestamp_object.year >= 2025:
                    if is_filing_old:
                        if isinstance(record.get('price_transaction'), dict):
                            # print(f'Converting price_transaction from dict to list for record ID: {record.get("id")}')
                            final_filing_output.append(record)
                    else: 
                        final_filing_output.append(record)

            return final_filing_output
    
    except Exception as error:
        print(f"Error fetching idx_filing_v2 data: {error}")
        raise


def match_data_filing(idx_filing: list[dict], idx_filing_v2: list[dict]) -> list[dict]: 
    matching_filings = []
    matching_filings_v2 = []
    not_matching_filings = []
    match_map = {}

    try: 
        primary_lookup = {}
        fallback_lookup = {}
        third_fallback_lookup = {}
        fourth_fallback_lookup = {}
        fifth_fallback_lookup = {}

        old_record_id_map = {} 
        uid_to_old_ids = {}

        for filing in idx_filing: 
            primary_key = create_composite_key(filing)
            second_key = create_composite_key_second_fallback(filing)
            third_key = create_composite_key_third_fallback(filing)
            third_key = create_composite_key_third_fallback(filing)
            fourth_key = create_composite_key_fourth_fallback(filing)
            fifth_key = create_composite_key_fifth_fallback(filing)
            
            # Store the actual record and its ID for tracking
            primary_lookup[primary_key] = filing
            fallback_lookup[second_key] = filing
            third_fallback_lookup[third_key] = filing
            fourth_fallback_lookup[fourth_key] = filing
            fifth_fallback_lookup[fifth_key] = filing

            # Map ID to record for the final separation step
            old_record_id_map[filing['id']] = filing

            uid = filing.get('uid')
            if uid: 
                uid_to_old_ids.setdefault(uid, []).append(filing['uid'])

        matched_old_ids = set()

        # Iterate V2 and check Primary -> then Fallback
        for filing_v2 in idx_filing_v2: 
            primary_key = create_composite_key(filing_v2)
            second_key = create_composite_key_second_fallback(filing_v2)
            third_key = create_composite_key_third_fallback(filing_v2)
            fourth_key = create_composite_key_fourth_fallback(filing_v2)
            fifth_key = create_composite_key_fifth_fallback(filing_v2)

            matched_record = None

            # Primary Key
            if primary_key in primary_lookup:
                matched_record = primary_lookup[primary_key]
            
            # Fallback Key (only if primary failed)
            elif second_key in fallback_lookup:
                matched_record = fallback_lookup[second_key]
            
            elif third_key in third_fallback_lookup:
                matched_record = third_fallback_lookup[third_key]

            elif fourth_key in fourth_fallback_lookup:
                matched_record = fourth_fallback_lookup[fourth_key]
            
            elif fifth_key in fifth_fallback_lookup:
                matched_record = fifth_fallback_lookup[fifth_key]

            if matched_record:
                matching_filings_v2.append(filing_v2)
                matched_old_ids.add(matched_record['id'])

                match_map[matched_record['id']] = filing_v2

                uid = matched_record.get('UID')
                if uid:
                    for old_id in uid_to_old_ids.get(uid, []):
                        matched_old_ids.add(old_id)
                        match_map.setdefault(old_id, filing_v2)

        # Separate Old Filings based on ID
        for old_id, filing in old_record_id_map.items():
            if old_id in matched_old_ids:
                matching_filings.append(filing)
            else:
                not_matching_filings.append(filing)

        print(f'Total matching filings: {len(matching_filings)}')
        print(f'Total not matching filings: {len(not_matching_filings)}')

        return matching_filings, matching_filings_v2, not_matching_filings, match_map

    except Exception as error:
        print(f"Error matching data filing: {error}")
        raise

def match_data_filing(idx_filing: list[dict], idx_filing_v2: list[dict]) -> list[dict]:
    matching_filings = []
    matching_filings_v2 = []
    not_matching_filings = []
    match_map = {}

    try:
        primary_lookup = {}
        fallback_lookup = {}
        third_fallback_lookup = {}
        fourth_fallback_lookup = {}

        old_record_id_map = {}
        uid_to_old_ids = {}

        for filing in idx_filing:
            primary_key = create_composite_key(filing)
            second_key = create_composite_key_second_fallback(filing)
            third_key = create_composite_key_third_fallback(filing)
            fourth_key = create_composite_key_fourth_fallback(filing)

            primary_lookup[primary_key] = filing
            fallback_lookup[second_key] = filing
            third_fallback_lookup[third_key] = filing
            fourth_fallback_lookup[fourth_key] = filing

            old_record_id_map[filing['id']] = filing

            uid = filing.get('UID')
            if uid:
                uid_to_old_ids.setdefault(uid, []).append(filing['id'])

        matched_old_ids = set()

        for filing_v2 in idx_filing_v2:
            primary_key = create_composite_key(filing_v2)
            second_key = create_composite_key_second_fallback(filing_v2)
            third_key = create_composite_key_third_fallback(filing_v2)
            fourth_key = create_composite_key_fourth_fallback(filing_v2)

            matched_record = None

            if primary_key in primary_lookup:
                matched_record = primary_lookup[primary_key]
            elif second_key in fallback_lookup:
                matched_record = fallback_lookup[second_key]
            elif third_key in third_fallback_lookup:
                matched_record = third_fallback_lookup[third_key]
            elif fourth_key in fourth_fallback_lookup:
                matched_record = fourth_fallback_lookup[fourth_key]

            if matched_record:
                matching_filings_v2.append(filing_v2)

                # Always mark the directly matched record
                matched_old_ids.add(matched_record['id'])
                match_map[matched_record['id']] = filing_v2

                # Also mark all old records with the same UID
                uid = matched_record.get('UID')
                if uid:
                    for old_id in uid_to_old_ids.get(uid, []):
                        matched_old_ids.add(old_id)
                        match_map.setdefault(old_id, filing_v2)

        for old_id, filing in old_record_id_map.items():
            if old_id in matched_old_ids:
                matching_filings.append(filing)
            else:
                not_matching_filings.append(filing)

        print(f'Total matching filings: {len(matching_filings)}')
        print(f'Total not matching filings: {len(not_matching_filings)}')

        return matching_filings, matching_filings_v2, not_matching_filings, match_map

    except Exception as error:
        print(f"Error matching data filing: {error}")
        raise


def update_old_to_new_format(old_filing: list[dict[str, any]]) -> list[dict[str, any]]: 
    try: 
        for filing in old_filing: 
            price_transaction = filing.get('price_transaction', [])
            if isinstance(price_transaction, dict): 
                length_prices = len(price_transaction.get('prices', [])) 
                
                price_transaction_list = []
                for index in range(length_prices):
                    price_transaction_list.append({
                        'type': price_transaction['type'][index] if 'type' in price_transaction else 'unknown',
                        'price': price_transaction['prices'][index],
                        'amount_transacted': price_transaction['amount_transacted'][index]
                    })

                filing['price_transaction'] = price_transaction_list    

        return old_filing
    
    except Exception as error:
        print(f"Error updating price transactions old filing to new format: {error}")
        raise


def update_from_filing_v2(
    match_map: dict[str, any], 
    old_filings: list[dict[str, any]]
) -> list[dict[str, any]]: 
    try: 
        for filing in old_filings: 
            old_id = filing.get('id')
            old_price_transactions = filing.get('price_transaction')

            if old_id in match_map: 
                v2_filing = match_map[old_id]

                v2_transactions = v2_filing.get('price_transaction', [])
                for index in range(len(old_price_transactions)): 
                    if old_price_transactions[index].get('type') == 'unknown': 
                        type_value = v2_transactions[index].get('type')
                        old_price_transactions[index]['type'] = type_value

                    date_value = v2_transactions[index].get('date')
                    old_price_transactions[index]['date'] = date_value

        return old_filings

    except Exception as error: 
        print(f'[update_from_filing_v2] Error: {error}')
        raise 


def detect_idx_format(source: str) -> bool:
    res = requests.get(source)
    res.raise_for_status()

    # doc = fitz.open(source) 
    # texts = doc[0].get_text()
    # print(texts)


def download_pdf_url(url: str, out_dir: str = "doc_transaction_update", retries: int = 3) -> Path | None:
    if not url:
        return None

    init_http(insecure=True, silence_warnings=True, load_env=True)
    out_dir_path = ensure_dir(out_dir)
    filename = safe_filename_from_url(url)
    out_path = out_dir_path / filename

    if out_path.exists():
        return out_path

    try:
        out_path.write_bytes(get_pdf_bytes_minimal(url, timeout=60))
        return out_path
    except Exception:
        pass

    for _ in range(max(0, retries - 1)):
        try:
            out_path.write_bytes(seed_and_retry_minimal(url, timeout=60))
            return out_path
        except Exception:
            continue

    return None


def download_pdfs_from_json(json_path: str, out_dir: str = "doc_transaction_update") -> None:
    with open(json_path, "r", encoding="utf-8") as f:
        data_records = json.load(f)

    print(f'total to process: {len(data_records)}')
    for index, data in enumerate(data_records):
        url = data.get("source")
        if not url:
            continue
        print(f'processing: {index} {url}')
        download_pdf_url(url, out_dir=out_dir)




if __name__ == '__main__':
    # supabase_client = get_supabase_client()
    # filing = get_idx_filing_data(supabase_client, 'idx_filings')
    # check_length_old_filing(filing)

    # filing_v2 = get_idx_filing_data(supabase_client, 'idx_filings_v2', False)
    # matching_filings_old, matching_filings_v2, not_matching_filings, match_map = ( 
    #     match_data_filing(filing, filing_v2)
    # )
    # updated_matching_old = update_old_to_new_format(matching_filings_old)
    # filing_updated_from_v2 = update_from_filing_v2(match_map, updated_matching_old)

    # inspect_duplicates(filing, filing_v2)

    # write_to_json(match_map, 'test_price_transaction/matched_map.json')
    # write_to_json(filing_updated_from_v2, 'test_price_transaction/idx_filing_updated_from_v2_2025.json')
    # write_to_json(matching_filings_old, 'test_price_transaction/matched_idx_filings_old_2025.json')
    # write_to_json(matching_filings_v2, 'test_price_transaction/matched_idx_filings_v2_2025.json')
    # write_to_json(not_matching_filings, 'test_price_transaction/not_matched_idx_filings_old_recent_2025.json')
    # write_to_json(updated_matching_old, 'test_price_transaction/updated_matched_idx_filing_old_2025.json')

    # test_source = 'https://www.idx.co.id/StaticData/NewsAndAnnouncement/ANNOUNCEMENTSTOCK/From_EREP/202507/4e0de956e5_c10ba09db9.pdf'
    # detect_idx_format(test_source)

    download_pdfs_from_json('test_price_transaction/not_matched_idx_filings_old_recent_2025.json')

    # uv run -m src.scripts.price_transaction_normalizer.update_price_transaction