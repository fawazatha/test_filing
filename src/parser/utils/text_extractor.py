import re
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

EN_DATE_PATTERN = (
        r"(?:\d{1,2})\s+"
        r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+"
        r"\d{4}"
    )

class TextExtractor:
    """Utility class for extracting various data from text using patterns."""
    
    DATE_PATTERN = r"(?:\d{1,2})\s+(?:Januari|Februari|Maret|April|Mei|Juni|Juli|Agustus|September|Oktober|November|Desember)\s+\d{4}"

    
    def __init__(self, text: str):
        self.lines = [line.strip() for line in text.splitlines() if line.strip()]
        self.text = text
    
    def find_table_value(self, keyword: str) -> str:
        """Find value in table-like structure."""
        for i, line in enumerate(self.lines):
            if keyword.lower() in line.lower():
                # Try splitting by whitespace
                parts = re.split(r'\s{3,}|\t+', line.strip())
                if len(parts) >= 2:
                    value = parts[-1].strip()
                    if value.lower() != keyword.lower() and len(value) > 1:
                        return value
                
                # Try regex pattern
                pattern = re.compile(rf"{re.escape(keyword)}\s+(.+)", re.IGNORECASE)
                match = pattern.search(line)
                if match:
                    value = match.group(1).strip()
                    if len(value) > 1:
                        return value
                
                # Look in next lines
                for j in range(i + 1, min(i + 3, len(self.lines))):
                    if self.lines[j] and not self._is_skip_line(self.lines[j]):
                        return self.lines[j].strip()
        return ""
    
    def find_value_after_keyword(self, keyword: str) -> str:
        """Find value in lines after keyword."""
        for i, line in enumerate(self.lines):
            if keyword.lower() in line.lower():
                for j in range(i + 1, min(i + 3, len(self.lines))):
                    if self.lines[j] and not self._is_skip_line(self.lines[j]):
                        return self.lines[j].strip()
        return ""
    
    def find_value_after_exact_line(self, keyword: str) -> str:
        """Find value in the line immediately after exact match."""
        for i, line in enumerate(self.lines):
            if line.strip().lower() == keyword.lower():
                if i + 1 < len(self.lines):
                    return self.lines[i + 1].strip()
        return ""
    
    def find_value_in_line(self, keyword: str) -> str:
        """Find value in the same line as keyword."""
        for line in self.lines:
            if keyword.lower() in line.lower():
                parts = re.split(r'\s{2,}|\t+', line.strip(), maxsplit=1)
                if len(parts) == 2:
                    return parts[1].strip()
        return ""
    
    def find_number_after_keyword(self, keyword: str) -> str:
        """Find number after keyword."""
        pattern = re.compile(rf"{re.escape(keyword)}\s*:?\s*([0-9\.,]+)", re.IGNORECASE)
        for line in self.lines:
            match = pattern.search(line)
            if match:
                return match.group(1).strip()
        
        # Fallback: look in next lines
        for i, line in enumerate(self.lines):
            if keyword.lower() in line.lower():
                for j in range(i + 1, min(i + 3, len(self.lines))):
                    if self.lines[j]:
                        number_match = re.search(r'([0-9\.,]+)', self.lines[j])
                        if number_match:
                            return number_match.group(1)
        return ""
    
    def find_percentage_after_keyword(self, keyword: str) -> str:
        """Find percentage after keyword."""
        pattern = re.compile(rf"{re.escape(keyword)}\s*:?\s*([0-9\.,]+)%?", re.IGNORECASE)
        for line in self.lines:
            match = pattern.search(line)
            if match:
                return match.group(1).strip()
        
        # Fallback: look in next lines
        for i, line in enumerate(self.lines):
            if keyword.lower() in line.lower():
                for j in range(i + 1, min(i + 3, len(self.lines))):
                    if self.lines[j]:
                        percent_match = re.search(r'([0-9\.,]+)%?', self.lines[j])
                        if percent_match:
                            return percent_match.group(1)
        return ""

    def extract_transaction_rows(self) -> List[Dict[str, Any]]:
        """Extract transaction rows from text (supports ID & EN)."""
        transactions: List[Dict[str, Any]] = []
        # 1) Coba mode tabel EN jelas: ada header "Type of Transaction"
        header_idx = -1
        for i, line in enumerate(self.lines):
            low = line.lower()
            if ("type of transaction" in low and "transaction price" in low) or \
               ("jenis transaksi" in low and "harga transaksi" in low):
                header_idx = i
                break

        from .number_parser import NumberParser

        def push_row(kind: str, price_s: str, date_s: str | None, amount_s: str):
            kind = kind.strip().lower()
            if kind in ("buy", "pembelian"):
                tx_type = "buy"
            elif kind in ("sell", "penjualan"):
                tx_type = "sell"
            else:
                return
            price = NumberParser.parse_number(price_s)
            amount = NumberParser.parse_number(amount_s)
            transactions.append({
                "type": tx_type,
                "price": price,
                "amount": amount,
                "value": price * amount,
                # simpan tanggal apa adanya; downstream bisa normalisasi
                "date": (date_s or "").strip()
            })

        if header_idx >= 0:
            j = header_idx + 1
            stop_tokens = ("purposes of transaction", "tujuan transaksi", "share ownership status",
                           "status kepemilikan saham", "respectfully", "hormat")
            while j < len(self.lines):
                row = (self.lines[j] or "").strip()
                if not row:
                    break
                low = row.lower()
                if any(tok in low for tok in stop_tokens):
                    break

                # Format umum EN: "Buy  420  13 August 2025  800.000"
                m = re.match(
                    rf"^\s*(Buy|Sell|Pembelian|Penjualan)\s+([0-9\.,]+)\s+({EN_DATE_PATTERN}|{self.DATE_PATTERN})?\s+([0-9\.,]+)\s*$",
                    row, re.IGNORECASE
                )
                if m:
                    kind, price_s, date_s, amt_s = m.group(1), m.group(2), m.group(3), m.group(4)
                    push_row(kind, price_s, date_s, amt_s)
                else:
                    # fallback: split kolom kaku
                    cols = re.split(r"\s{2,}|\t+|\s{1,}", row)
                    if len(cols) >= 4:
                        push_row(cols[0], cols[1], cols[-2] if len(cols) >= 5 else None, cols[-1])
                j += 1

            return transactions

        # 2) Fallback: baris terpisah tanpa header (ID/EN)
        for line in self.lines:
            row = (line or "").strip()
            jenis = row.split(" ", 1)[0].lower()
            if any(k in jenis for k in ("pembelian", "penjualan", "buy", "sell")):
                # Ambil angka pertama = price, angka terakhir = amount
                nums = re.findall(r"[0-9][0-9\.,]*", row)
                if len(nums) >= 2:
                    price_s, amount_s = nums[0], nums[-1]
                    date_match = re.search(rf"{EN_DATE_PATTERN}|{self.DATE_PATTERN}", row, re.IGNORECASE)
                    date_s = date_match.group(0) if date_match else None
                    push_row(jenis, price_s, date_s, amount_s)

        return transactions

    
    def contains_transfer_transaction(self) -> bool:
        """Check if text contains transfer transaction."""
        for line in self.lines:
            if any(kw in line.lower() for kw in ["jenis transaksi", "transaction type"]):
                continue
            if "pengalihan" in line.lower():
                return True
        return False
    
    def extract_transfer_transactions(self, ticker: str) -> List[Dict[str, Any]]:
        """Extract transfer transactions from text."""
        import uuid
        from datetime import datetime
        from .number_parser import NumberParser
        
        transfer_rows = []
        for line in self.lines:
            if "pengalihan" in line.lower():
                date_match = re.search(self.DATE_PATTERN, line)
                price_match = re.search(r'\b\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{1,2})?\b', line)
                amount_match = re.findall(r'\b\d{1,3}(?:[.,]\d{3})+\b', line)

                if date_match and amount_match:
                    date_str = date_match.group(0)
                    try:
                        normalized_date = datetime.strptime(date_str, "%d %B %Y").strftime("%Y%m%d")
                    except:
                        normalized_date = date_str.replace(" ", "").lower()

                    price = NumberParser.parse_number(price_match.group(0)) if price_match else 0
                    if len(amount_match) >= 1:
                        amt = amount_match[-1]
                        amount = NumberParser.parse_number(amt)
                        uid_str = f"{ticker}-{normalized_date}-{amount}-{price}"
                        transfer_uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, uid_str))
                        transfer_rows.append({
                            "type": "transfer",
                            "price": price,
                            "amount": amount,
                            "value": price * amount,
                            "transfer_uid": transfer_uid
                        })
        return transfer_rows
    
    def _is_skip_line(self, line: str) -> bool:
        """Check if line should be skipped."""
        skip_keywords = [':', 'nama', 'kode', 'jumlah', 'persentase', 'jenis', 'tanggal']
        return any(skip in line.lower() for skip in skip_keywords)
