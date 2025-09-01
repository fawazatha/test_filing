import re
from typing import Union, Optional

class NumberParser:
    """Utility class for parsing numbers and percentages from text."""
    
    @staticmethod
    def parse_number(s: Union[str, int, float], is_percentage: bool = False) -> Union[int, float]:
        """
        Parse number or percentage from string.
        If is_percentage=True, delegate to parse_percentage().
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
            # Hanya koma
            if cleaned.count(',') > 1:
                # Ribuan saja: 1,234,567 -> 1234567
                normalized = cleaned.replace(',', '')
            else:
                # Satu koma: cek 3 digit -> kemungkinan ribuan
                comma_pos = cleaned.find(',')
                after_comma = cleaned[comma_pos + 1:]
                if after_comma.isdigit() and len(after_comma) == 3:
                    normalized = cleaned.replace(',', '')
                else:
                    # Anggap desimal: 12,34 -> 12.34
                    normalized = cleaned.replace(',', '.')

        elif '.' in cleaned:
            # Hanya titik
            if cleaned.count('.') > 1:
                # Multi titik -> jelas ribuan: 1.234.567 -> 1234567
                normalized = cleaned.replace('.', '')
            else:
                # Satu titik: kalau 3 digit setelah titik, treat as ribuan
                dot_pos = cleaned.find('.')
                before_dot = cleaned[:dot_pos]
                after_dot = cleaned[dot_pos + 1:]
                # abaikan tanda minus saat cek digit sebelum titik
                before_is_digits = before_dot.replace('-', '').isdigit()
                if after_dot.isdigit() and len(after_dot) == 3 and before_is_digits:
                    # Contoh: 1.110 -> 1110, 16.700 -> 16700
                    normalized = cleaned.replace('.', '')
                else:
                    # Contoh desimal US: 106.6 -> 106.6
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
        Parse string persen ( '0,45%' -> 0.45, '45%' -> 45.0 ).
        Catatan: Untuk persen, titik tunggal SELALU dianggap desimal (5.001 -> 5.001),
        jangan diperlakukan sebagai pemisah ribuan.
        """
        if s is None:
            return 0.0

        txt = str(s).replace('%', '').strip()
        if txt == "":
            return 0.0

        # Keep only digits, comma, dot, minus (buang teks lain)
        txt = re.sub(r'[^0-9,.\-]', '', txt)

        if ',' in txt and '.' in txt:
            # Gunakan posisi separator terakhir (EU vs US style)
            last_comma = txt.rfind(',')
            last_dot = txt.rfind('.')
            if last_comma > last_dot:
                # EU/ID: 1.234,567 -> 1234.567
                normalized = txt.replace('.', '').replace(',', '.')
            else:
                # US: 1,234.567 -> 1234.567
                normalized = txt.replace(',', '')
        elif ',' in txt:
            # Koma saja -> selalu desimal utk persen
            normalized = txt.replace(',', '.')
        elif '.' in txt:
            if txt.count('.') > 1:
                # Banyak titik: hapus titik ribuan, sisakan titik terakhir sebagai desimal
                parts = txt.split('.')
                normalized = ''.join(parts[:-1]) + '.' + parts[-1]
            else:
                # Satu titik: ANGGAP DESIMAL (5.001 -> 5.001)
                normalized = txt
        else:
            normalized = txt

        try:
            return float(normalized)
        except Exception:
            return 0.0
