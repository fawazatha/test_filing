import re
from typing import Union, Optional
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

def _to_decimal(x):
    """
    Convert input to Decimal safely. Returns None on invalid input.
    """
    if x in (None, ""):
        return None
    try:
        return Decimal(str(x))
    except (InvalidOperation, TypeError, ValueError):
        return None

def _quantize_pct5(d: Decimal) -> Decimal:
    """
    Quantize a Decimal to at most 5 decimal places (ROUND_HALF_UP),
    then normalize to remove trailing zeros.
    """
    return d.quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP).normalize()


class NumberParser:
    """Utility class for parsing numbers and percentages from text."""
    
    @staticmethod
    def parse_number(s: Union[str, int, float], is_percentage: bool = False) -> Union[int, float]:
        """
        Parse number or percentage from a string.
        If is_percentage=True, delegate to parse_percentage().

        Notes:
        - Keeps your existing normalization decisions for thousand/decimal separators.
        - Returns int when the numeric value is an integer (e.g., 1000.0 -> 1000).
        - Returns 0 on invalid/empty inputs.
        """
        if is_percentage:
            return NumberParser.parse_percentage(s)
        
        if not s or str(s).strip() == "":
            return 0
        
        # Keep digits, comma, dot, minus
        cleaned = re.sub(r'[^0-9,.\-]', '', str(s))

        if ',' in cleaned and '.' in cleaned:
            last_comma = cleaned.rfind(',')
            last_dot = cleaned.rfind('.')
            if last_comma > last_dot:
                # ID/EU style: 1.234.567,89 -> 1234567.89
                normalized = cleaned.replace('.', '').replace(',', '.')
            else:
                # US style: 1,234,567.89 -> 1234567.89
                normalized = cleaned.replace(',', '')

        elif ',' in cleaned:
            # Comma only
            if cleaned.count(',') > 1:
                # Multiple commas -> thousands only: 1,234,567 -> 1234567
                normalized = cleaned.replace(',', '')
            else:
                # Single comma: if exactly 3 digits after comma (and digits before),
                # likely a thousands separator; otherwise treat as decimal.
                comma_pos = cleaned.find(',')
                after_comma = cleaned[comma_pos + 1:]
                if after_comma.isdigit() and len(after_comma) == 3:
                    normalized = cleaned.replace(',', '')
                else:
                    normalized = cleaned.replace(',', '.')

        elif '.' in cleaned:
            # Dot only
            if cleaned.count('.') > 1:
                # Multiple dots -> thousands only: 1.234.567 -> 1234567
                normalized = cleaned.replace('.', '')
            else:
                # Single dot: if exactly 3 digits after dot (and valid digits before),
                # likely a thousands separator; otherwise treat as decimal.
                dot_pos = cleaned.find('.')
                before_dot = cleaned[:dot_pos]
                after_dot = cleaned[dot_pos + 1:]
                # ignore minus sign when checking digits before dot
                before_is_digits = before_dot.replace('-', '').isdigit()
                if after_dot.isdigit() and len(after_dot) == 3 and before_is_digits:
                    # e.g., 1.110 -> 1110, 16.700 -> 16700
                    normalized = cleaned.replace('.', '')
                else:
                    # e.g., decimal US: 106.6 -> 106.6
                    normalized = cleaned
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
        Parse percentage string (e.g., '0,45%' -> 0.45, '45%' -> 45.0).

        Important:
        - Keeps your existing normalization logic for comma/dot handling:
          * If both comma and dot exist, the last one determines the decimal separator.
          * Comma-only -> treat as decimal separator.
          * Single dot -> always treat as decimal (5.001 -> 5.001).
          * Multiple dots -> last dot is decimal, earlier dots are thousands separators.
        - Final conversion uses Decimal with ROUND_HALF_UP to at most 5 decimals
          to avoid float noise like 0.289999999.
        - Returns float (no forced trailing zeros). Returns 0.0 on failure.
        """
        if s is None:
            return 0.0

        # Remove '%' and surrounding spaces. NBSP is rare but safe to handle.
        txt = str(s).replace('\u00A0', ' ').replace('%', '').strip()
        if txt == "":
            return 0.0

        # Keep only digits, comma, dot, minus
        txt = re.sub(r'[^0-9,.\-]', '', txt)

        # === Existing normalization logic retained as-is ===
        if ',' in txt and '.' in txt:
            # Use position of the last separator to pick decimal symbol
            last_comma = txt.rfind(',')
            last_dot = txt.rfind('.')
            if last_comma > last_dot:
                # EU/ID: 1.234,567 -> 1234.567
                normalized = txt.replace('.', '').replace(',', '.')
            else:
                # US: 1,234.567 -> 1234.567
                normalized = txt.replace(',', '')
        elif ',' in txt:
            # Comma only -> treat as decimal separator for percentages
            normalized = txt.replace(',', '.')
        elif '.' in txt:
            if txt.count('.') > 1:
                # Multiple dots: keep last as decimal; others are thousands separators
                parts = txt.split('.')
                normalized = ''.join(parts[:-1]) + '.' + parts[-1]
            else:
                # Single dot: treat as decimal (do NOT treat as thousands)
                normalized = txt
        else:
            normalized = txt

        # === Safe final conversion using Decimal (max 5 decimals, HALF_UP) ===
        d = _to_decimal(normalized)
        if d is None:
            return 0.0
        try:
            q = _quantize_pct5(d)
            return float(q)  # stays float; 0.29 remains 0.29, not '0.29000'
        except InvalidOperation:
            return 0.0