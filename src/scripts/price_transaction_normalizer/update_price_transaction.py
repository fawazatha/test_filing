from supabase import create_client, Client 
from dotenv import load_dotenv
from datetime import datetime 
from pathlib import Path
from collections import defaultdict
from decimal import Decimal, InvalidOperation

from src.downloader.client import init_http, get_pdf_bytes_minimal, seed_and_retry_minimal
from src.common.files import ensure_dir, safe_filename_from_url
from src.scripts.price_transaction_normalizer.old_parser import generate_article_filings

import os
import json 
import fitz
import requests
import copy 


load_dotenv(override=True)
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY') 


def write_to_json(data: list, filename: str) -> None:
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    except Exception as error:
        print(f"Error writing to JSON file: {error}")
        raise


def load_from_json(filename: str):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
        
    except Exception as error:
        print(f"Error loading JSON file: {error}")
        raise


def truncate_to_minute(timestamp_str):
    dt = datetime.fromisoformat(timestamp_str)
    return dt.replace(second=0, microsecond=0)


def check_length_old_filing(tag:str, filing_payload: list[dict[str, any]]): 
    count = 0
    print(f'\n total {tag}: {len(filing_payload)}')
    for filing in filing_payload:
        price_transaction = filing.get('price_transaction', []) 
        if isinstance(price_transaction, dict): 
            count += 1 
    print(f'Total {tag} with price_transaction as dict: {count}')
    

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


def get_idx_filing_data(
    supabase_client: Client, 
    table_name: str, 
    is_save: bool = False,
    year_minimum: int = 2025, 
    is_filing_old: bool = True
):
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
                if timestamp_object.year >= year_minimum:
                    if is_filing_old:
                        if isinstance(record.get('price_transaction'), dict):
                            # print(f'Converting price_transaction from dict to list for record ID: {record.get("id")}')
                            final_filing_output.append(record)
                    else: 
                        final_filing_output.append(record)

            if is_save:
                write_to_json(final_filing_output, f'test_price_transaction/legacy_data_filings_{table_name}.json')

            return final_filing_output
    
    except Exception as error:
        print(f"Error fetching idx_filing_v2 data: {error}")
        raise


def match_data_filing_v2(idx_filing: list[dict], idx_filing_v2: list[dict]) -> list[dict]:
    matching_filings = []
    matching_filings_v2 = []
    not_matching_filings = []
    match_map = {}

    try:
        primary_lookup = {}
        fallback_lookup = {}
        third_fallback_lookup = {}
        fourth_fallback_lookup = {}
        # fifth_fallback_lookup = {}

        old_record_id_map = {}
        uid_to_old_ids = {}

        for filing in idx_filing:
            primary_key = create_composite_key(filing)
            second_key = create_composite_key_second_fallback(filing)
            third_key = create_composite_key_third_fallback(filing)
            fourth_key = create_composite_key_fourth_fallback(filing)
            # fifth_key = create_composite_key_fifth_fallback(filing)

            primary_lookup[primary_key] = filing
            fallback_lookup[second_key] = filing
            third_fallback_lookup[third_key] = filing
            fourth_fallback_lookup[fourth_key] = filing
            # fifth_fallback_lookup[fifth_key] = filing

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
            # fifth_key = create_composite_key_fifth_fallback(filing_v2)

            matched_record = None

            if primary_key in primary_lookup:
                matched_record = primary_lookup[primary_key]
            elif second_key in fallback_lookup:
                matched_record = fallback_lookup[second_key]
            elif third_key in third_fallback_lookup:
                matched_record = third_fallback_lookup[third_key]
            elif fourth_key in fourth_fallback_lookup:
                matched_record = fourth_fallback_lookup[fourth_key]
            # elif fifth_key in fifth_fallback_lookup:
            #     matched_record = fifth_fallback_lookup[fifth_key]

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

        print(f'\nTotal matching filings: {len(matching_filings)}')
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


def _norm_num(v):
    if v is None:
        return None
    
    if isinstance(v, (int, float, Decimal)):
        return Decimal(str(v))
    
    s = str(v).strip()
    if not s:
        return None
    
    try:
        return Decimal(s)
    except InvalidOperation:
        return s  


def update_from_filing_v2(
    match_map: dict[str, any], 
    old_filings: list[dict[str, any]]
) -> list[dict[str, any]]: 
    try: 
        skipped = 0
        for filing in old_filings: 
            old_id = filing.get('id')
            old_tx = filing.get("price_transaction") or []

            if not old_id or old_id not in match_map or not old_tx:
                continue
            
            v2_filing = match_map[old_id]
            v2_tx = v2_filing.get('price_transaction')

            for key in ("share_percentage_before", "share_percentage_after", "share_percentage_transaction"):
                if filing.get(key) is None and v2_filing.get(key) is not None:
                    filing[key] = v2_filing.get(key)

            if not v2_tx:
                print(f'skipped: {old_id}')
                skipped += 1
                continue
            
            buckets = defaultdict(list)
            for tx in v2_tx:
                key = (_norm_num(tx.get("price")), _norm_num(tx.get("amount_transacted")), tx.get("type"))
                buckets[key].append(tx)

            for tx in old_tx:
                price_key = _norm_num(tx.get("price"))
                amount_key = _norm_num(tx.get("amount_transacted"))
                old_type = tx.get("type")

                matched = None
                if old_type and old_type != "unknown":
                    key = (price_key, amount_key, old_type)
                    if buckets.get(key):
                        matched = buckets[key].pop(0)

                else:
                    # fallback: ignore type
                    for key, vals in buckets.items():
                        if key[0] == price_key and key[1] == amount_key and vals:
                            matched = vals.pop(0)
                            break

                if matched:
                    if tx.get("type") == "unknown":
                        tx["type"] = matched.get("type")
                    tx["date"] = matched.get("date")

                

        print(f"Skipped (no v2 tx): {skipped}")
        return old_filings


            # if old_id in match_map: 
            #     v2_filing = match_map[old_id]

            #     v2_transactions = v2_filing.get('price_transaction', [])

            #     for index in range(len(old_price_transactions)): 
            #         if old_price_transactions[index].get('type') == 'unknown': 
            #             type_value = v2_transactions[index].get('type')
            #             old_price_transactions[index]['type'] = type_value

            #         date_value = v2_transactions[index].get('date')
            #         old_price_transactions[index]['date'] = date_value

        return old_filings

    except Exception as error: 
        print(f'[update_from_filing_v2] Error: {error}')
        raise 


def detect_idx_format(base_dir: str) -> tuple:
    documents = os.listdir(base_dir)
    print(f'length document to check: {len(documents)}')

    detected_idx_format = []
    detected_non_idx_format = []

    for document in documents:
        print(f'\nopening: {document}')

        doc = fitz.open(f'{base_dir}/{document}') 
        texts = doc[0].get_text()
        
        if len(texts) < 1:
            print(f'adding non idx, cant open the file: {document}')
            detected_non_idx_format.append(document)

        if "Go To English Page" in texts:
            detected_idx_format.append(document)
        else:
            if document not in detected_non_idx_format:
                detected_non_idx_format.append(document)

    print(f'\nlength idx detected: {len(detected_idx_format)}')
    print(f'\nlength non idx detected: {len(detected_non_idx_format)}')

    write_to_json(detected_idx_format, 'test_price_transaction/matched_idx_format.json')
    write_to_json(detected_non_idx_format, 'test_price_transaction/non_matched_non_idx_format.json')

    return detected_idx_format, detected_non_idx_format


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


# def _extract_price_amount_pairs(filing: dict) -> list[tuple]:
#     pt = filing.get("price_transaction")
#     if isinstance(pt, dict):
#         prices = pt.get("prices") or []
#         amounts = pt.get("amount_transacted") or []
#         length = min(len(prices), len(amounts))
#         return [(prices[i], amounts[i]) for i in range(length)]
#     if isinstance(pt, list):
#         return [(item.get("price"), item.get("amount_transacted")) for item in pt]
#     return []


# def validate_price_transaction_update(
#     before_old: list[dict],
#     after_old: list[dict],
#     match_map: dict,
#     strict: bool = True,
#     max_report: int = 20,
# ) -> dict:
#     before_map = {f["id"]: _extract_price_amount_pairs(f) for f in before_old if f.get("id") is not None}
#     after_map = {f["id"]: _extract_price_amount_pairs(f) for f in after_old if f.get("id") is not None}
#     after_filing_map = {f["id"]: f for f in after_old if f.get("id") is not None}

#     price_mismatches = []
#     for old_id, before_pairs in before_map.items():
#         after_pairs = after_map.get(old_id)
#         if after_pairs is None or before_pairs != after_pairs:
#             price_mismatches.append(old_id)

#     date_mismatches = []
#     for old_id, v2 in match_map.items():
#         after_filing = after_filing_map.get(old_id)
#         if not after_filing:
#             continue
#         v2_tx = v2.get("price_transaction") or []
#         after_tx = after_filing.get("price_transaction") or []

#         if len(v2_tx) != len(after_tx):
#             date_mismatches.append(old_id)
#             continue

#         for i in range(len(v2_tx)):
#             v2_date = v2_tx[i].get("date")
#             if v2_date and after_tx[i].get("date") != v2_date:
#                 date_mismatches.append(old_id)
#                 break

#     print(f"Validation: price_mismatches={len(price_mismatches)}, date_mismatches={len(date_mismatches)}")
#     if price_mismatches:
#         print(f"Price mismatches (sample): {price_mismatches[:max_report]}")
#     if date_mismatches:
#         print(f"Date mismatches (sample): {date_mismatches[:max_report]}")

#     if strict and (price_mismatches or date_mismatches):
#         raise ValueError("Validation failed; see mismatches above.")

#     return {
#         "price_mismatches": price_mismatches,
#         "date_mismatches": date_mismatches,
#     }


# def fix_price_transaction_mismatch(
#     result_validation: dict[str],
#     filing_updated_from_v2: list[dict],
#     match_map: dict
# ) -> list[dict]:
#     # lookup
#     filing_updated_map = {
#         filing.get("id"): filing 
#         for filing in filing_updated_from_v2 if filing.get("id") is not None
#     }

#     date_mismatches = result_validation.get("date_mismatches") or []
#     fixed_ids = []
    
#     for old_id in date_mismatches:
#         v2_filing = match_map.get(old_id)
#         if not v2_filing:
#             continue

#         v2_price_transaction = v2_filing.get("price_transaction") or []
#         if not v2_price_transaction:
#             print(f'\n skipping non: {old_id}')
#             continue

#         old_filing = filing_updated_map.get(old_id)
#         if not old_filing:
#             continue

#         old_filing["price_transaction"] = v2_price_transaction
#         old_filing["price"] = v2_filing.get("price")
#         old_filing["transaction_value"] = v2_filing.get("transaction_value")
#         old_filing["amount_transaction"] = v2_filing.get("amount_transaction")
#         old_filing["transaction_type"] = v2_filing.get("transaction_type")

#         fixed_ids.append(old_id)

#     print(f"Fixed mismatches: {len(fixed_ids)}")
#     return filing_updated_from_v2


def parse_old_idx(json_path: str):
    list_filename = load_from_json(json_path)
    print(list_filename[:2])


def upsert_price_transactions(
    supabase_client: Client,
    table_name: str,
    rows: list[dict],
    batch_size: int = 500,
) -> None:
    if not rows:
        print("No rows to upsert.")
        return

    total = len(rows)
    for start in range(0, total, batch_size):
        batch = rows[start:start + batch_size]

        supabase_client.table(table_name).upsert(batch, on_conflict="id").execute()

        print(f"Upserted {min(start + batch_size, total)}/{total}")


def run_main(base_dir: str):
    supabase_client = get_supabase_client()
    
    filing = get_idx_filing_data(supabase_client, 'idx_filings', is_save=True)
    check_length_old_filing('filing', filing)

    filing_v2 = get_idx_filing_data(
        supabase_client,
        "idx_filings_v2",
        year_minimum=2025,
        is_filing_old=False,
    )
    check_length_old_filing('filing_v2', filing_v2)
    
    matching_filings_old, matching_filings_v2, not_matching_filings, match_map = ( 
        match_data_filing_v2(filing, filing_v2)
    )
    updated_matching_old = update_old_to_new_format(matching_filings_old)
    filing_updated_from_v2 = update_from_filing_v2(match_map, updated_matching_old)

    print("\nmatching_filings_old:", len(matching_filings_old))
    print("matching_filings_v2:", len(matching_filings_v2))
    print("not_matching_filings:", len(not_matching_filings))
    print(f"match_map: {len(match_map)}\n")


    write_to_json(match_map, f'{base_dir}/matched_map.json')
    write_to_json(filing_updated_from_v2, f'{base_dir}/idx_filing_updated_from_v2_2025.json')
    write_to_json(matching_filings_old, f'{base_dir}/matched_idx_filings_old_2025.json')
    write_to_json(matching_filings_v2, f'{base_dir}/matched_idx_filings_v2_2025.json')
    write_to_json(not_matching_filings, f'{base_dir}/not_matched_idx_filings_old_recent_2025.json')
    write_to_json(updated_matching_old, f'{base_dir}/updated_matched_idx_filing_old_2025.json')


if __name__ == '__main__':
    new_base_dir = 'test_new_price_transaction'
    base_dir = 'test_price_transaction'
    # run_main(new_base_dir)

    # old_snapshot = copy.deepcopy(matching_filings_old)
    # result_validation = validate_price_transaction_update(
    #     old_snapshot, filing_updated_from_v2, match_map, strict=False
    # )

    # list_mismatch = result_validation.get('date_mismatches')
    # mismatch_ids = set(list_mismatch)

    # filtered = [f for f in matching_filings_old if f.get("id") in mismatch_ids]
    # print(f'length: {len(filtered)}')
    # write_to_json(filtered, "test_price_transaction/idx_filing_mismatches.json")

    # print(f'\ntry fixing mismatch')
    # fixed_filing_updated_from_v2 = fix_price_transaction_mismatch(
    #     result_validation, filing_updated_from_v2, match_map
    # )

    # validate_price_transaction_update(
    #     old_snapshot, filing_updated_from_v2, match_map, strict=False
    # )


    # inspect_duplicates(filing, filing_v2)

   
    # write_to_json(fixed_filing_updated_from_v2, 'test_price_transaction/fixed_idx_filing_updated_from_v2.json')
    # write_to_json(filing_updated_from_v2, 'test_price_transaction/unknown_idx_filing_updated_from_v2.json')


    # others_output = 'downloads/doc_mismatch'
    # others_input = 'output_others.json'
    # input = 'test_price_transaction/not_matched_idx_filings_old_recent_2025.json'
    # mismatch_input = 'test_price_transaction/idx_filing_mismatches.json'

    not_matched_new = 'test_new_price_transaction/not_matched_idx_filings_old_recent_2025.json'
    not_matched_new_doc_path = 'downloads/doc_not_match'
    download_pdfs_from_json(json_path=not_matched_new, out_dir=not_matched_new_doc_path)

    # Detect idx format in non matched
    # documents_path = 'downloads/doc_transaction'
    # detect_idx_format(documents_path)

    # parse non matched for idx format 
    # parse_old_idx('test_price_transaction/list_matched_idx_format.json')


    # uv run -m src.scripts.price_transaction_normalizer.update_price_transaction