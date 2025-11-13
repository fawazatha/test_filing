import re
from typing import Optional, Union

from decimal import Decimal, ROUND_FLOOR, InvalidOperation

def _to_decimal(x):
    if x in (None, ""): return None
    try: return Decimal(str(x))
    except (InvalidOperation, TypeError, ValueError): return None

def _floor_pct5(d: Decimal) -> Decimal:
    return ( (d * Decimal("1e5")).to_integral_value(rounding=ROUND_FLOOR) / Decimal("1e5") ).normalize()

def pct_close(a, b, tol_pp=Decimal("0.00050")) -> bool:
    """
    Absolute tolerance compare in percentage points (e.g., 0.29000 vs 0.29049).
    Default tol = 0.00050 (== 0.05pp).
    """
    da, db = _to_decimal(a), _to_decimal(b)
    if da is None or db is None:
        return False
    return abs(da - db) <= _to_decimal(tol_pp)

def safe_div(n, d) -> Optional[Decimal]:
    n, d = _to_decimal(n), _to_decimal(d)
    if n is None or d in (None, Decimal(0)):
        return None
    return (n / d).normalize()

class NumberParser:
    """Centralized class for parsing locale-aware number strings."""

    @staticmethod
    def parse_number(s: Union[str, int, float], is_percentage: bool = False) -> Union[int, float]:
        """
        Parses a string that could have commas or dots as decimal/thousands separators.
        """
        if is_percentage:
            return NumberParser.parse_percentage(s)
        if not s or str(s).strip() == "":
            return 0
        
        cleaned = re.sub(r'[^0-9,.\-]', '', str(s))
        
        if ',' in cleaned and '.' in cleaned:
            # Disambiguate based on last separator
            last_comma = cleaned.rfind(',')
            last_dot = cleaned.rfind('.')
            # If comma is last (e.g., 1.234,56), treat dot as thousand sep
            normalized = cleaned.replace('.', '').replace(',', '.') if last_comma > last_dot else cleaned.replace(',', '')
        elif ',' in cleaned:
            # If only commas, check if it's a single decimal (1,23) or thousands (1,234,567)
            normalized = cleaned.replace(',', '.') if cleaned.count(',') == 1 and len(cleaned.split(',')[-1]) != 3 else cleaned.replace(',', '')
        elif '.' in cleaned:
            # If only dots, check if it's a single decimal (1.23) or thousands (1.234.567)
            normalized = cleaned if cleaned.count('.') == 1 and len(cleaned.split('.')[-1]) != 3 else cleaned.replace('.', '')
        else:
            normalized = cleaned
            
        try:
            val = float(normalized)
            return int(val) if val.is_integer() else val
        except ValueError:
            return 0

    @staticmethod
    def parse_percentage(s: Union[str, float]) -> float:
        """
        Parses a percentage string, which often uses comma as decimal.
        """
        if s is None:
            return 0.0
        
        txt = re.sub(r'[^0-9,.\-]', '', str(s).replace('%', '').strip())
        
        if ',' in txt and '.' in txt:
            last_comma = txt.rfind(',')
            last_dot = txt.rfind('.')
            normalized = txt.replace('.', '').replace(',', '.') if last_comma > last_dot else txt.replace(',', '')
        elif ',' in txt:
            # In percentages, comma is almost always a decimal
            normalized = txt.replace(',', '.')
        elif '.' in txt:
            # Handle multiple dots (e.g. 1.2.3 -> 12.3) - keep last as decimal
            normalized = ''.join(txt.split('.')[:-1]) + '.' + txt.split('.')[-1] if txt.count('.') > 1 else txt
        else:
            normalized = txt
            
        d = _to_decimal(normalized)
        if d is None:
            return 0.0
        try:
            q = _floor_pct5(d)
            return float(q)  
        except InvalidOperation:
            return 0.0
        except (TypeError, ValueError):
            return 0.0
