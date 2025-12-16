from supabase import create_client, Client 
from dotenv import load_dotenv
from datetime import datetime 

import os
import json 


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

        old_record_id_map = {} 

        for filing in idx_filing: 
            primary_key = create_composite_key(filing)
            second_key = create_composite_key_second_fallback(filing)
            third_key = create_composite_key_third_fallback(filing)
            
            # Store the actual record and its ID for tracking
            primary_lookup[primary_key] = filing
            fallback_lookup[second_key] = filing
            third_fallback_lookup[third_key] = filing

            # Map ID to record for the final separation step
            old_record_id_map[filing['id']] = filing

        matched_old_ids = set()

        # Iterate V2 and check Primary -> then Fallback
        for filing_v2 in idx_filing_v2: 
            primary_key = create_composite_key(filing_v2)
            second_key = create_composite_key_second_fallback(filing_v2)
            third_key = create_composite_key_third_fallback(filing_v2)

            matched_record = None

            # Primary Key
            if primary_key in primary_lookup:
                matched_record = primary_lookup[primary_key]
            
            # Fallback Key (only if primary failed)
            elif second_key in fallback_lookup:
                matched_record = fallback_lookup[second_key]
            
            elif third_key in third_fallback_lookup:
                matched_record = third_fallback_lookup[third_key]

            if matched_record:
                matching_filings_v2.append(filing_v2)
                matched_old_ids.add(matched_record['id'])

                match_map[matched_record['id']] = filing_v2

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


if __name__ == '__main__':
    supabase_client = get_supabase_client()
    filing = get_idx_filing_data(supabase_client, 'idx_filings')
    check_length_old_filing(filing)

    filing_v2 = get_idx_filing_data(supabase_client, 'idx_filings_v2', False)
    matching_filings_old, matching_filings_v2, not_matching_filings, match_map = match_data_filing(filing, filing_v2)
    updated_matching_old = update_old_to_new_format(matching_filings_old)
    filing_updated_from_v2 = update_from_filing_v2(match_map, updated_matching_old)

    # inspect_duplicates(filing, filing_v2)

    write_to_json(match_map, 'matched_map.json')
    write_to_json(filing_updated_from_v2, 'idx_filing_updated_from_v2_2025.json')
    write_to_json(matching_filings_old, 'matched_idx_filings_old_2025.json')
    write_to_json(matching_filings_v2, 'matched_idx_filings_v2_2025.json')
    write_to_json(not_matching_filings, 'not_matched_idx_filings_old_2025.json')
    write_to_json(updated_matching_old, 'updated_matched_idx_filing_old_2025.json')
