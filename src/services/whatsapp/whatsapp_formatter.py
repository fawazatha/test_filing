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
        symbol = filing.get("symbol", "-")
        holder_name = filing.get("holder_name", "-")
        transaction_type = filing.get("transaction_type", "-").upper()
        
        # Format financial data with short notation
        price = format_abbreviated_idr(filing.get("price"))
        value = format_abbreviated_idr(filing.get("transaction_value"))
        amount = format_number_abbreviated(filing.get("amount"))
        
        tx_percentage = format_tx_percentage(filing.get("share_percentage_transaction"))

        # Get timestamp and source
        time = filing.get("display_time", filing.get("timestamp", "-"))
        source = filing.get("source", "-")
        
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


def format_number_abbreviated(value: any, decimal_places: int = 1) -> str:
    if value is None or value == '':
        return '-'
    
    try:
        num = float(value) 

        is_negative = num < 0
        num = abs(num)

        TRILLION = 1_000_000_000_000
        BILLION = 1_000_000_000
        MILLION = 1_000_000
        THOUSAND = 1_000

        if num >= TRILLION:
            abbreviated = num / TRILLION
            suffix = "T"
        elif num >= BILLION:
            abbreviated = num / BILLION
            suffix = "B"
        elif num >= MILLION:
            abbreviated = num / MILLION
            suffix = "M"
        elif num >= THOUSAND:
            abbreviated = num / THOUSAND
            suffix = "K"
        else:
            abbreviated = num
            suffix = ""

        formatted = f"{abbreviated:.{decimal_places}f}"

        if formatted.endswith(".0"):
            formatted = formatted[:-2]

        if is_negative:
            formatted = f"-{formatted}"
        
        return f"{formatted}{suffix}"

    except (ValueError, TypeError):
        return str(value)
    

def format_abbreviated_idr(value: any) -> str:
    abbreviated = format_number_abbreviated(value, decimal_places=1)
    
    if abbreviated == "—":
        return "—"
    
    return f"IDR {abbreviated}"


def format_tx_percentage(value: any) -> str: 
    if value is None or value == '':
        return '-'
    
    try:
        num = float(value)
        return f"{num:.2f}"
    except (ValueError, TypeError):
        return str(value)
    

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