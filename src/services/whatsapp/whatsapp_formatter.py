import json 

from src.services.whatsapp.utils.config import LOGGER


def format_payload(
        filing: dict[str, any], 
        total_companies: int, 
        total_filings: int,
        window_start: str, 
        window_end: str
    ) -> json:
    try:
        # Extract and validate required fields
        symbol = filing.get("symbol", "N/A")
        holder_name = filing.get("holder_name", "N/A")
        transaction_type = filing.get("transaction_type", "N/A").upper()
        
        # Format financial data with short notation
        price = format_idr_price(filing.get("price"))
        value = format_idr_price(filing.get("transaction_value"))
        amount = format_amount(filing.get("amount"))
        tx_percentage = format_tx_percentage(filing.get("share_percentage_transaction"))

        # Get timestamp and source
        time = filing.get("display_time", filing.get("timestamp", "N/A"))
        source = filing.get("source", "N/A")
        
        # Format window dates
        date_before, hour_before = format_window_date(window_start)
        date_after, hour_after = format_window_date(window_end)
        
        # Build the payload
        payload = {
            "date_before": str(date_before),
            "hour_before": str(hour_before),
            "date_after": str(date_after),
            "hour_after": str(hour_after),
            "total_companies": str(total_companies),
            "total_filings": str(total_filings),
            "company": symbol,
            "holder": holder_name,
            "type": transaction_type,
            "price": price,
            "value": value,
            "amount": amount,
            "tx": tx_percentage,
            "time": time,
            "source": source,
        }
        
        return json.dumps(payload)
        
    except Exception as error:
        LOGGER.error(f"Error formatting payload for filing {filing.get('id', 'unknown')}: {error}")
        return None


def format_idr_price(value: any) -> str:
    try:
        if value is None:
            return 'IDR 0'
        
        num_converted = float(value)
        return f'IDR {num_converted:.0f}'

    except (ValueError, TypeError):
        return 'IDR 0'
    

def format_amount(value: any) -> str:
    try:
        if value is None:
            return "0"
        
        num = float(value)
        
        if num == 0:
            return "0"
        
        if abs(num) >= 1_000_000_000:
            return f"{num / 1_000_000_000:.1f}B"
        
        if abs(num) >= 1_000_000:
            formatted = f"{num / 1_000_000:.1f}M"
            return formatted.replace(".0M", "M")
        
        if abs(num) >= 1_000:
            return f"{num / 1_000:.1f}K"
        
        return f"{num:.0f}"
        
    except (ValueError, TypeError):
        return "0"


def format_tx_percentage(value: any) -> str:
    try:
        if value is None:
            return "0"
        num = float(value)
   
        if num == int(num):
            return str(int(num))
        return f"{num}"
    
    except (ValueError, TypeError):
        return "0"


def format_window_date(value: str) -> str | str:  
    try: 
        if not value:
            LOGGER.warning("Window date value is empty or None.")
            date = "N/A"
            hour = "N/A"
            return date, hour
            
        date, hour = value.split(" ") 
        return date, hour

    except Exception as error: 
        LOGGER.error(f"Error formatting window date '{value}': {error}")
        date = "N/A"
        hour = "N/A"
        return date, hour
    

if __name__ == '__main__':
    # test usage
    filings_path = 'data/report/insider_report.json'
    formatted_data = format_payload(filings_path)