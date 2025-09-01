"""
Filing data extraction utilities.

This module handles the extraction of structured information from raw filing text.
It uses pattern matching and heuristics to identify key filing information.
"""

import re
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from ..model import FilingInfo

logger = logging.getLogger(__name__)


class FilingDataExtractor:
    """
    Extracts structured data from raw filing text.
    
    This class uses various pattern matching techniques to identify and extract
    key information from Indonesian financial filing documents.
    """

    def __init__(self):
        """Initialize the filing data extractor."""
        # Indonesian month names mapping
        self.bulan_map = {
            "Januari": "01", "Februari": "02", "Maret": "03", 
            "April": "04", "Mei": "05", "Juni": "06",
            "Juli": "07", "Agustus": "08", "September": "09",
            "Oktober": "10", "November": "11", "Desember": "12"
        }

    def extract_filing_info(self, text: str) -> FilingInfo:
        """
        Extract filing information from raw text.

        Args:
            text (str): Raw filing text

        Returns:
            FilingInfo: Structured filing information

        Raises:
            ValueError: If text is empty or invalid
        """
        if not text or not text.strip():
            raise ValueError("Filing text cannot be empty")

        logger.debug("Starting filing data extraction")

        try:
            lines = text.split("\n")
            extracted_data = self._extract_basic_info(lines)
            extracted_data.update(self._extract_financial_data(lines))
            extracted_data.update(self._extract_transaction_data(lines))

            filing_info = FilingInfo(**extracted_data)
            
            logger.debug(f"Successfully extracted filing info for: {filing_info.holder_name}")
            return filing_info

        except Exception as e:
            logger.error(f"Error extracting filing information: {e}")
            # Return empty filing info on error
            return FilingInfo()

    def _extract_basic_info(self, lines: List[str]) -> Dict[str, Any]:
        """
        Extract basic filing information (names, numbers, etc.).

        Args:
            lines (List[str]): Lines from the filing text

        Returns:
            Dict[str, Any]: Basic information dictionary
        """
        info = {
            "document_number": "",
            "company_name": "",
            "holder_name": "",
            "ticker": "",
            "category": "",
            "control_status": "",
            "purpose": "",
            "date_time": ""
        }

        # Mapping of labels to fields where value is in previous line
        label_prevline = {
            "Nomor Surat": "document_number",
            "Nama Perusahaan": "company_name",
            "Kode Emiten": "ticker",
        }

        for i, line in enumerate(lines):
            # Extract values from previous line pattern
            for label, field in label_prevline.items():
                if label in line and i > 0 and not info[field]:
                    prev_line = lines[i - 1].strip()
                    if prev_line:
                        info[field] = prev_line

            # Extract holder name
            if "Nama Pemegang Saham" in line and not info["holder_name"]:
                info["holder_name"] = self._extract_holder_name(line)

            # Extract category
            if "Kategori" in line and not info["category"]:
                info["category"] = self._extract_after_label(line, "Kategori")

            # Extract control status
            if any(status in line for status in ["Status Pengedali", "Status Pengendali"]) and not info["control_status"]:
                pattern = r"Status Pengendali|Status Pengedali"
                info["control_status"] = self._extract_after_pattern(line, pattern)

            # Extract purpose
            if "Tujuan Transaksi" in line and not info["purpose"]:
                info["purpose"] = self._extract_after_label(line, "Tujuan Transaksi")

            # Extract date and time
            if any(dt_label in line for dt_label in ["Tanggal dan Waktu", "Date and Time"]) and not info["date_time"]:
                info["date_time"] = self._extract_datetime(line, lines, i)

        return info

    def _extract_financial_data(self, lines: List[str]) -> Dict[str, Any]:
        """
        Extract financial data (shareholdings, percentages).

        Args:
            lines (List[str]): Lines from the filing text

        Returns:
            Dict[str, Any]: Financial information dictionary
        """
        info = {
            "shareholding_before": "",
            "shareholding_after": "",
            "share_percentage_before": "",
            "share_percentage_after": "",
            "share_percentage_transaction": ""
        }

        field_mapping = {
            "Jumlah Saham Sebelum Transaksi": "shareholding_before",
            "Jumlah Saham Setelah Transaksi": "shareholding_after",
            "Persentase Saham Sebelum Transaksi": "share_percentage_before",
            "Persentase Saham Sesudah Transaksi": "share_percentage_after",
            "Persentase Saham yang ditransaksi": "share_percentage_transaction",
        }

        for line in lines:
            for label, field in field_mapping.items():
                if label in line and not info[field]:
                    info[field] = self._extract_last_numeric_token(line)

        return info

    def _extract_transaction_data(self, lines: List[str]) -> Dict[str, Any]:
        """
        Extract transaction data (prices, amounts).

        Args:
            lines (List[str]): Lines from the filing text

        Returns:
            Dict[str, Any]: Transaction information dictionary
        """
        info = {
            "price_transaction": {"prices": [], "amount_transacted": []}
        }

        # Look for transaction table
        for i, line in enumerate(lines):
            if "Jenis Transaksi Harga Transaksi" in line and i + 2 < len(lines):
                prices, amounts = self._extract_transaction_table(lines, i + 2)
                info["price_transaction"]["prices"] = prices
                info["price_transaction"]["amount_transacted"] = amounts
                break

        return info

    def _extract_transaction_table(self, lines: List[str], start_idx: int) -> tuple:
        """
        Extract transaction table data starting from given index.

        Args:
            lines (List[str]): Lines from the filing text
            start_idx (int): Starting index for table data

        Returns:
            tuple: (prices, amounts) lists
        """
        prices = []
        amounts = []

        for j in range(start_idx, len(lines)):
            row = lines[j].strip()
            if not row:
                break

            # Check if this is a transaction row
            if not re.match(r"(Pembelian|Penjualan)\b", row):
                break

            # Extract price (second token after transaction type)
            parts = row.split()
            if len(parts) >= 2:
                price = self._extract_number(parts[1])
                if price is not None:
                    prices.append(price)

            # Extract amount (last numeric token)
            amount = self._extract_number(self._extract_last_numeric_token(row))
            if amount is not None:
                amounts.append(amount)

        return prices, amounts

    def _extract_holder_name(self, line: str) -> str:
        """Extract holder name from line containing 'Nama Pemegang Saham'."""
        after_label = line.split("Nama Pemegang Saham", 1)[-1].strip(" :")
        tokens = after_label.split()
        # Remove common words like 'adalah'
        filtered_tokens = [t for t in tokens if t.lower() not in ['adalah', 'yaitu']]
        return " ".join(filtered_tokens).strip()

    def _extract_after_label(self, line: str, label: str) -> str:
        """Extract text after a specific label."""
        return line.split(label, 1)[-1].strip(" :")

    def _extract_after_pattern(self, line: str, pattern: str) -> str:
        """Extract text after a regex pattern."""
        parts = re.split(pattern, line, 1)
        return parts[-1].strip(" :") if len(parts) > 1 else ""

    def _extract_last_numeric_token(self, line: str) -> str:
        """Extract the last numeric token from a line."""
        if not line:
            return ""
        
        tokens = [t for t in re.split(r"\s+", line.strip()) if t]
        for token in reversed(tokens):
            if re.search(r"[\d,\.%]", token):
                return token
        return ""

    def _extract_datetime(self, line: str, lines: List[str], line_idx: int) -> str:
        """
        Extract datetime from current line or next line.

        Args:
            line (str): Current line
            lines (List[str]): All lines
            line_idx (int): Current line index

        Returns:
            str: Extracted datetime string
        """
        # Try current line first
        dt_guess = self._parse_datetime_patterns(line)
        
        # If not found, try next line
        if not dt_guess and line_idx + 1 < len(lines):
            dt_guess = self._parse_datetime_patterns(lines[line_idx + 1])
            
        return dt_guess

    def _parse_datetime_patterns(self, text: str) -> str:
        """
        Parse various datetime patterns from text.

        Args:
            text (str): Text containing potential datetime

        Returns:
            str: Parsed datetime string or empty string
        """
        if not text:
            return ""

        # Pattern 1: dd-mm-YYYY HH:MM(:SS)?
        match = re.search(r"\b(\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}(?::\d{2})?)\b", text)
        if match:
            return match.group(1)

        # Pattern 2: dd <Indonesian Month> YYYY
        pattern = r"\b(\d{1,2})\s+(Januari|Februari|Maret|April|Mei|Juni|Juli|Agustus|September|Oktober|November|Desember)\s+(\d{4})\b"
        match = re.search(pattern, text)
        if match:
            day, month_name, year = match.groups()
            day = day.zfill(2)
            month = self.bulan_map[month_name]
            return f"{day}-{month}-{year} 00:00"

        return ""

    def _extract_number(self, input_string: str) -> Optional[int]:
        """
        Extract integer number from string.

        Args:
            input_string (str): String potentially containing a number

        Returns:
            Optional[int]: Extracted number or None
        """
        if not input_string:
            return None
            
        try:
            cleaned = input_string.replace(".", "").replace(",", "")
            match = re.search(r"\d+", cleaned)
            return int(match.group()) if match else None
        except (ValueError, AttributeError):
            return None