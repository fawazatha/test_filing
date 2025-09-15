import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# --- Date/number helpers ---
BULAN_MAP = {
    "Januari": "01", "Februari": "02", "Maret": "03", "April": "04", "Mei": "05",
    "Juni": "06", "Juli": "07", "Agustus": "08", "September": "09",
    "Oktober": "10", "November": "11", "Desember": "12"
}

def extract_datetime(text: str) -> str:
    """Try multiple datetime patterns and normalize to 'dd-mm-YYYY HH:MM:SS' or 'dd-mm-YYYY HH:MM'.
    Supports:
      - '31-08-2025 14:05(:30)?'
      - '31 Agustus 2025'
    Returns empty string if not found.
    """
    if not text:
        return ""
    m = re.search(r"\b(\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}(?::\d{2})?)\b", text)
    if m:
        return m.group(1)
    m2 = re.search(
        r"\b(\d{1,2})\s+(Januari|Februari|Maret|April|Mei|Juni|Juli|Agustus|September|Oktober|November|Desember)\s+(\d{4})\b",
        text,
    )
    if m2:
        dd, mon_name, yyyy = m2.groups()
        dd = dd.zfill(2)
        mm = BULAN_MAP[mon_name]
        return f"{dd}-{mm}-{yyyy} 00:00"
    return ""

def extract_number(input_string: str) -> Optional[int]:
    """Extract the first integer-like token (digits with optional separators)."""
    if not input_string:
        return None
    input_string = input_string.replace(".", "").replace(",", "")
    m = re.search(r"(\d+)", input_string)
    return int(m.group(1)) if m else None

def _last_numeric_token(s: str) -> str:
    tokens = re.findall(r"\d+[\d,.]*", s)
    return tokens[-1] if tokens else ""

# --- Info extraction from text (layout-heuristics friendly) ---
def extract_info(text: str) -> Dict[str, Any]:
    lines = (text or "").split("\n")
    info: Dict[str, Any] = {
        "document_number": "",
        "company_name": "",
        "holder_name": "",
        "ticker": "",
        "category": "",
        "control_status": "",
        "transactions": [],
        "shareholding_before": "",
        "shareholding_after": "",
        "share_percentage_before": "",
        "share_percentage_after": "",
        "share_percentage_transaction": "",
        "purpose": "",
        "date_time": "",
        "price": 0,
        "price_transaction": {"prices": [], "amount_transacted": []},
    }

    label_prevline = {
        "Nomor Surat": "document_number",
        "Nama Perusahaan": "company_name",
        "Kode Emiten": "ticker",
    }

    map_fields = {
        "Jumlah Saham Sebelum": "shareholding_before",
        "Jumlah Saham Setelah": "shareholding_after",
        "Jumlah Saham Berubah": "share_percentage_transaction",
        "Persentase Saham Sebelum": "share_percentage_before",
        "Persentase Saham Setelah": "share_percentage_after",
    }

    for i, line in enumerate(lines):
        line = (line or "").strip()

        # 1) Generic 'Label: value' pattern
        m = re.match(r"^([^:]+):\s*(.+)$", line)
        if m:
            label, val = m.groups()
            if label in ("Pemegang Saham", "Holder Name"):
                info["holder_name"] = val.strip()
            elif label in ("Kode Emiten", "Ticker") and not info["ticker"]:
                info["ticker"] = val.strip().replace(".JK", "")
            elif label in ("Kategori", "Category"):
                info["category"] = val.strip()
            elif label in ("Status Pengendali", "Control Status"):
                info["control_status"] = val.strip()
            elif label in ("Tujuan Transaksi", "Purpose"):
                info["purpose"] = val.strip()

        # 2) Previous line carries label (PDF layout quirk)
        for label, field in label_prevline.items():
            if label in line and i > 0 and not info[field]:
                info[field] = lines[i - 1].strip()

        # 3) Numeric tables
        for label, field in map_fields.items():
            if label in line and not info[field]:
                info[field] = _last_numeric_token(line)

        # 4) Date / time
        if ("Tanggal dan Waktu" in line or "Date and Time" in line) and not info["date_time"]:
            dt_guess = extract_datetime(line)
            if not dt_guess and i + 1 < len(lines):
                dt_guess = extract_datetime(lines[i + 1])
            info["date_time"] = dt_guess or info["date_time"]

        # 5) Price table block
        if ("Jenis Transaksi Harga Transaksi" in line) and (i + 2 < len(lines)):
            prices: List[int] = []
            amounts: List[int] = []
            for j in range(i + 2, len(lines)):
                row = lines[j].strip()
                if not row:
                    break
                m_tx = re.match(r"(Pembelian|Penjualan)\b", row)
                if not m_tx:
                    break
                parts = row.split()
                # crude heuristic: last number-like is amount, previous number-like is price
                nums = re.findall(r"\d+", row.replace(".", ""))
                if len(nums) >= 2:
                    price = int(nums[-2])
                    amt = int(nums[-1])
                    prices.append(price)
                    amounts.append(amt)
            if prices and amounts and len(prices) == len(amounts):
                info["price_transaction"] = {"prices": prices, "amount_transacted": amounts}
                # 'price' fallback: use last price
                info["price"] = prices[-1]

    return info
