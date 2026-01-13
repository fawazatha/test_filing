from supabase import create_client
from dotenv import load_dotenv

from src.scripts.price_transaction_normalizer.update_price_transaction import get_idx_filing_data
from src.core.transformer import _generate_title_and_body

import os 
import json 


load_dotenv(override=True)
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY') 


def update_title_and_body(client, type_target: str = 'others'): 
    filings = get_idx_filing_data(client, 'idx_filings', 2026, False)

    c = 0
    urls_others = []

    for filing in filings:
        transaction_type = filing.get('transaction_type')

        if not transaction_type:
            print(f'type nan: {transaction_type}')
            continue     
       
        if transaction_type == type_target:
            holder_name = filing.get('holder_name')
            company_name = filing.get('company_name')
            tx_type = transaction_type
            amount = filing.get('amount_transaction')
            holding_before = filing.get('holding_before')
            holding_after = filing.get('holding_after')
            purpose_en = None
            source = filing.get('source')

            print(f'processing url: {source}')
            urls_others.append(source)

            title, _ = _generate_title_and_body(
                holder_name, company_name, tx_type, amount,
                holding_before, holding_after, purpose_en
            )

            print(title)
            c += 1

    print(c)

    with open("output_others.json", "w", encoding="utf-8") as f:
        json.dump(urls_others, f, ensure_ascii=False, indent=2)


if __name__ == '__main__':
    client = create_client(supabase_url=SUPABASE_URL, supabase_key=SUPABASE_KEY)
    update_title_and_body(client) 

# uv run -m src.scripts.update_title_others                                  
