from supabase import create_client
from dotenv import load_dotenv
from pathlib import Path

from src.scripts.price_transaction_normalizer.update_price_transaction import get_idx_filing_data
from src.core.transformer import _generate_title_and_body, translator
from src.parser.parser_idx_new import parser_new_document

import os 
import json 


load_dotenv(override=True)
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY') 


# def update_title_and_body(client, type_target: str = 'others'): 
#     filings = get_idx_filing_data(client, 'idx_filings', 2026, False)

#     c = 0
#     urls_others = []

#     for filing in filings:
#         transaction_type = filing.get('transaction_type')

#         if not transaction_type:
#             print(f'type nan: {transaction_type}')
#             continue     
       
#         if transaction_type == type_target:
#             holder_name = filing.get('holder_name')
#             company_name = filing.get('company_name')
#             tx_type = transaction_type
#             amount = filing.get('amount_transaction')
#             holding_before = filing.get('holding_before')
#             holding_after = filing.get('holding_after')
#             purpose_en = None
#             source = filing.get('source')

#             print(f'processing url: {source}')
#             urls_others.append(source)

#             title, _ = _generate_title_and_body(
#                 holder_name, company_name, tx_type, amount,
#                 holding_before, holding_after, purpose_en
#             )

#             print(title)
#             c += 1

#     print(c)

#     with open("output_others.json", "w", encoding="utf-8") as f:
#         json.dump(urls_others, f, ensure_ascii=False, indent=2)

def get_idx_filing_data(
        supabase_client, 
        table_name: str = 'idx_filings', 
        start_date: str = '2025-12-01', 
        end_date: str = '2025-12-30', 
        is_filing_old: bool = False
    ):
    try: 
        response = (
            supabase_client
            .table(table_name)
            .select('*')
            .gte('timestamp', f'{start_date}T00:00:00')
            .lte('timestamp', f'{end_date}T23:59:59')
            .execute()
        )

        final_filing_output = []
        if response.data: 
            for record in response.data:
                if is_filing_old:
                    if isinstance(record.get('price_transaction'), dict):
                        final_filing_output.append(record)
                else: 
                    final_filing_output.append(record)

            return final_filing_output
    
    except Exception as error:
        print(f"Error fetching idx_filing_v2 data: {error}")
        raise

def get_others_record(client, type_target: str = 'others') -> list[dict]: 
    filings = get_idx_filing_data(client)

    count = 0
    record_others = []

    for filing in filings:
        transaction_type = filing.get('transaction_type')

        if not transaction_type:
            print(f'type nan: {transaction_type}')
            continue     
       
        if transaction_type == type_target:
            record_others.append(filing)
            count+=1 

    print(f'total others: {count}')

    return record_others


def get_parser(record_others: list[dict]) -> list[dict[dict]]:
    base_dir = 'downloads/idx-format'

    sources = os.listdir(base_dir)
    
    source_map = {
        os.path.basename(source).lower(): f'{base_dir}/{source}'
        for source in sources
    }

    print(f'len list sources: {len(source_map)}')
    print(f'len record others: {len(record_others)}')

    result_parser = []
    for record in record_others:
        source = record.get('source')
        source_name = os.path.basename(source)
        source_name = source_name.lower()
        
      
        path_doc = source_map.get(source_name)

        if path_doc is None:
            print(f'No file found for: {source_name}')
            continue

        print(f'parsing doc: {path_doc}')

        result_others, result_no_others = parser_new_document(path_doc)

        print(f'Type of result_others: {type(result_others)}')
        print(f'result_others: {result_others}')

        purpose = result_others.get('purpose')
        result_others['purpose'] = translator(purpose)

        result_parser.append({
            os.path.basename(source): {
                'others': result_others,
                'no_others': result_no_others
            }
        })

    print(f'result parsing: {len(result_parser)}')
    return result_parser


def update_title(record_db, record_parser): 
    parser_lookup = {}
    for parser_item in record_parser:
        # Each item is a dict with filename as key
        for filename, data in parser_item.items():
            parser_lookup[filename.lower()] = data
    
    updated_records = {}

    for record in record_db:
        source_db = record.get('source')
        source_name_db = os.path.basename(source_db).lower()

        # Find matching parser record
        parser_data = parser_lookup.get(source_name_db)
        
        if not parser_data:
            print(f'No parser match found for: {source_name_db}')
            continue

        # Get purpose from 'others' data
        others_data = parser_data.get('others', {})
        purpose_en = others_data.get('purpose')
        company_name = others_data.get('company_name')
        
        if not purpose_en:
            print(f'No purpose found in parser for: {source_name_db}')
            continue

        holder_name = record.get('holder_name')
        tx_type = record.get('transaction_type')
        amount = record.get('amount_transaction')
        holding_before = record.get('holding_before')
        holding_after = record.get('holding_after')
        source = record.get('source')

        print(f'Processing: {source}')

        # Generate new title with purpose
        title, _ = _generate_title_and_body(
            holder_name, company_name, tx_type, amount,
            holding_before, holding_after, purpose_en
        )
        
        # Store updated record
        updated_records.update({
            int(record.get('id')): title
        })
    
    return updated_records


def upsert_others_data(client, record_db: list[dict], update_title: dict):
    try:
        for record in record_db:
            id = record.get('id')
            new_title = update_title.get(id)
            record['title'] = new_title

        response = client.table('idx_filings').upsert(record_db).execute()

        print(f'\nfinal: {json.dumps(record_db, indent=2)}')
        return response.data
    
    except Exception as error:
        print(f"Error upserting data: {error}")
        return None

if __name__ == '__main__':
    client = create_client(supabase_url=SUPABASE_URL, supabase_key=SUPABASE_KEY)
    # update_title_and_body(client) 

    record_others = get_others_record(client)
    print(f'\n{json.dumps(record_others, indent=2)}')
    
    result_parser = get_parser(record_others)
    print(f'\n{json.dumps(result_parser, indent=2)}')

    updated = update_title(record_others, result_parser)
    
    print(f'\nUpdated {len(updated)} records')
    print(json.dumps(updated, indent=2))

    # res = upsert_others_data(client, record_others, updated)

    # print(f'\nfinal res: {res}\n')

    # print(record_others[:1])
    # print(os.listdir('downloads/idx-format'))

    

    #'output_others.json'

# uv run -m src.scripts.update_title_others                                  
