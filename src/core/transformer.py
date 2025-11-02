# src/core/transformer.py
from __future__ import annotations
import re
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from src.core.types import FilingRecord, PriceTransaction

from src.common.strings import (
    to_float,
    to_int,
    kebab,
    strip_diacritics
)

# Constants
TAG_WHITELIST = {
    "takeover", "mesop", "inheritance", "award", 
    "share-transfer", "internal-strategy"
}
PURPOSE_TAG_MAP = {
    "akuisisi": "takeover", "acquisition": "takeover",
    "strategi internal": "internal-strategy", "internal strategy": "internal-strategy",
    "pengembangan usaha": "internal-strategy", "business expansion": "internal-strategy",
    "mesop": "mesop", "warisan": "inheritance", "inheritance": "inheritance",
    "penghargaan": "award", "award": "award", "transfer": "share-transfer",
}

# Translation Placeholder
def _translate_to_english(text: str) -> str:
    if not text:
        return ""
    known_phrases = {
        "bagian dari proses akuisisi": "Part of the acquisition process",
        "strategi internal": "Internal strategy",
        "pengembangan usaha": "Business expansion",
        "investasi": "investment",
        "divestasi": "divestment",
    }
    normalized_text = text.strip().lower()
    if normalized_text in known_phrases:
        return known_phrases[normalized_text]
    if normalized_text == "investation":
        return "Investment"
    logging.warning(f"No translation found for '{text}'. Using original.")
    return text

# Type Coercion Helpers (Specific to this file)
def _to_str(x: Any) -> Optional[str]:
    """Robustly converts a value to a stripped string."""
    if x is None: return None
    return str(x).strip()


# PERBAIKAN: DUA FUNGSI TANGGAL

def _parse_date_obj(x: Any) -> Optional[datetime]:
    """Helper internal untuk mengubah input apa pun menjadi objek datetime."""
    if x is None or x == "": return None
    if isinstance(x, datetime): return x
    
    s = str(x).strip()
    
    # 1. Coba format YYYYMMDD (dari parser)
    if len(s) == 8 and s.isdigit():
        try:
            return datetime.strptime(s, "%Y%m%d")
        except Exception: pass 
    
    # 2. Coba format ISO (dari ingestion.json atau parser)
    try:
        return datetime.fromisoformat(s.replace(" ", "T"))
    except Exception: pass
    
    logging.warning(f"Could not parse date: {x}. Returning None.")
    return None

def _to_iso_date_full(x: Any) -> Optional[str]:
    """
    Mengonversi nilai tanggal/waktu menjadi string ISO LENGKAP (termasuk HH:MM:SS).
    e.g., "2025-10-26T22:55:13"
    """
    dt_obj = _parse_date_obj(x)
    if dt_obj:
        # Mengembalikan format ISO, .isoformat() sudah benar
        return dt_obj.isoformat()
    return None

def _to_iso_date_short(x: Any) -> Optional[str]:
    """
    Mengonversi nilai tanggal/waktu menjadi string YYYY-MM-DD (HANYA tanggal).
    e.g., "2025-10-23"
    """
    dt_obj = _parse_date_obj(x)
    if dt_obj:
        return dt_obj.strftime("%Y-%m-%d")
    return None
# AKHIR PERBAIKAN


def _normalize_symbol(sym: Any) -> Optional[str]:
    """Ensures symbol is UPPER.JK format."""
    s = _to_str(sym)
    if not s: return None
    s_upper = s.upper()
    return s_upper if s_upper.endswith(".JK") else f"{s_upper}.JK"

# Core Logic Functions

def _normalize_transaction_type(raw_type: Any, holding_before: Any, holding_after: Any) -> str:
    t = _to_str(raw_type)
    t = t.lower() if t else ""
    if t in {"buy", "sell", "share-transfer", "award", "inheritance", "mesop"}:
        return t
    hb = to_int(holding_before)
    ha = to_int(holding_after)
    if hb is not None and ha is not None:
        if ha > hb: return "buy"
        if ha < hb: return "sell"
    if "transfer" in t: return "share-transfer"
    return "other"

def _build_tx_list_from_list(tx_list: List[Dict[str, Any]], raw_date: Any) -> List[PriceTransaction]:
    """Builds PriceTransaction list from the new format (list of dicts)"""
    built_txs = []
    for tx in tx_list:
        if not isinstance(tx, dict): continue
        built_txs.append(PriceTransaction(
            transaction_date=_to_iso_date_short(tx.get("date") or raw_date), # Pakai format pendek
            transaction_type=_to_str(tx.get("type")),
            transaction_price=to_float(tx.get("price")),
            transaction_share_amount=to_int(tx.get("amount") or tx.get("amount_transacted"))
        ))
    return built_txs

def _build_tx_list_from_dict(tx_dict: Dict[str, Any], raw_date: Any) -> List[PriceTransaction]:
    """Builds PriceTransaction list from the old format (dict of lists)"""
    built_txs = []
    try:
        prices = tx_dict.get("prices", [])
        amounts = tx_dict.get("amount_transacted", [])
        types = tx_dict.get("type", []) 
        max_len = max(len(prices), len(amounts))
        for i in range(max_len):
            built_txs.append(PriceTransaction(
                transaction_date=_to_iso_date_short(raw_date), # Pakai format pendek
                transaction_type=_to_str(types[i]) if i < len(types) else "other",
                transaction_price=to_float(prices[i]) if i < len(prices) else None,
                transaction_share_amount=to_int(amounts[i]) if i < len(amounts) else None
            ))
        return built_txs
    except Exception:
        return []

def _calculate_wap_and_totals(
    tx_list: List[PriceTransaction],
) -> Tuple[Optional[float], Optional[int], Optional[float]]:
    total_value, total_amount = 0.0, 0
    for tx in tx_list:
        price, amount = tx.transaction_price, tx.transaction_share_amount
        tx_type = (tx.transaction_type or "").lower()
        if tx_type in {"buy", "sell"}:
             if price is not None and amount is not None and price > 0 and amount > 0:
                total_value += (price * amount)
                total_amount += amount
    if total_amount == 0:
        return None, None, None
    wap = total_value / total_amount
    return wap, total_amount, total_value

def _generate_title_and_body(
    holder_name: str, company_name: str, tx_type: str, 
    amount: Optional[int], holding_before: Optional[int],
    holding_after: Optional[int], purpose_en: str
) -> tuple[str, str]:
    action_title = tx_type.replace("-", " ").title()
    if tx_type == "buy": action_verb = "bought"
    elif tx_type == "sell": action_verb = "sold"
    elif tx_type == "share-transfer": action_verb = "transferred"
    elif tx_type == "award": action_verb = "was awarded"
    elif tx_type == "inheritance": action_verb = "inherited"
    else: action_verb = "executed a transaction for"
    title = f"{holder_name} {action_title} Transaction of {company_name}"
    amount_str = f"{amount:,} shares" if amount is not None else "shares"
    body = f"{holder_name} {action_verb} {amount_str} of {company_name}."
    if holding_before is not None and holding_after is not None:
        hb_str, ha_str = f"{holding_before:,}", f"{holding_after:,}"
        if holding_after > holding_before:
            body += f" This increases their holdings from {hb_str} to {ha_str} shares."
        elif holding_after < holding_before:
            body += f" This decreases their holdings from {hb_str} to {ha_str} shares."
        else:
            body += f" Their holdings remain at {ha_str} shares."
    if purpose_en:
        body += f" The stated purpose of the transaction was {purpose_en.lower()}."
    return title, body

def _normalize_tags(raw_tags: Any, purpose_en: str) -> List[str]:
    tags = set()
    tag_list = []
    if isinstance(raw_tags, list):
        tag_list = raw_tags
    elif isinstance(raw_tags, str):
        try: tag_list = json.loads(raw_tags)
        except Exception: tag_list = [t.strip() for t in raw_tags.split(",")]
    for tag in tag_list:
        t = _to_str(tag)
        if t:
            t_low = t.lower()
            if t_low in TAG_WHITELIST: tags.add(t_low)
    purpose_low = purpose_en.lower() if purpose_en else ""
    for key, tag in PURPOSE_TAG_MAP.items():
        if key in purpose_low: tags.add(tag)
    return sorted(list(tags))


# Public Transformer Function (UPDATED)

def transform_raw_to_record(
    raw_dict: Dict[str, Any], 
    ingestion_map: Dict[str, Dict[str, Any]] # Tipe diubah ke Dict[str, Dict]
) -> FilingRecord:
    """
    The main transformation function. Takes a raw dict from any
    source and converts it into the canonical FilingRecord.
    """
    raw_purpose = _to_str(raw_dict.get("purpose"))
    purpose_en = _translate_to_english(raw_purpose)
    
    holding_before = to_int(raw_dict.get("holding_before"))
    holding_after = to_int(raw_dict.get("holding_after"))
    
    tx_type = _normalize_transaction_type(
        raw_dict.get("transaction_type") or raw_dict.get("type"),
        holding_before, holding_after
    )
    
    pp_before = to_float(raw_dict.get("share_percentage_before"), ndigits=5)
    pp_after = to_float(raw_dict.get("share_percentage_after"), ndigits=5)
    pp_tx = to_float(raw_dict.get("share_percentage_transaction"), ndigits=5)
    
    # PERBAIKAN LOGIKA TANGGAL & SOURCE
    
    main_date = None
    main_source_url = None
    raw_filename = _to_str(raw_dict.get("source"))
    
    # Prioritas 1: Cek ingestion_map
    if raw_filename:
        ingestion_item = ingestion_map.get(raw_filename)
        if ingestion_item:
            main_date = ingestion_item.get("date") # e.g., "2025-10-26T22:55:13"
            main_source_url = ingestion_item.get("main_link") or ingestion_item.get("link")

    # Prioritas 2: Fallback ke data parser jika tidak ada di map
    if not main_date:
        main_date = (
            raw_dict.get("timestamp") or
            raw_dict.get("announcement_published_at")
        )
    # Prioritas 3: Fallback ke tanggal transaksi pertama
    if not main_date:
        txs_list_for_date = raw_dict.get("transactions")
        if isinstance(txs_list_for_date, list) and txs_list_for_date:
            main_date = txs_list_for_date[0].get("date")

    # Fallback untuk Source URL jika tidak ada di ingestion map
    if not main_source_url:
        main_source_url = _to_str(raw_dict.get("source") or raw_dict.get("pdf_url"))

    # 2. Build the transaction list
    price_tx_list = []
    if isinstance(raw_dict.get("transactions"), list):
        price_tx_list = _build_tx_list_from_list(
            raw_dict["transactions"], 
            main_date
        )
    elif isinstance(raw_dict.get("price_transaction"), dict):
        price_tx_list = _build_tx_list_from_dict(
            raw_dict["price_transaction"],
            main_date
        )

    # 3. Hitung WAP dan Total
    wap, total_amount_tx, total_value_tx = _calculate_wap_and_totals(price_tx_list)

    # 4. Tentukan nilai level atas
    amount = to_int(raw_dict.get("amount_transaction") or raw_dict.get("amount"))
    if amount is None: amount = total_amount_tx
        
    value = to_float(raw_dict.get("transaction_value"))
    if value is None: value = total_value_tx
    
    price = to_float(raw_dict.get("price"))
    if price is None and wap is not None: price = wap
    
    if value is None and price is not None and amount is not None:
        value = price * amount
        
    # AKHIR PERBAIKAN
        
    holder_name = _to_str(raw_dict.get("holder_name")) or "Unknown Shareholder"
    company_name = _to_str(raw_dict.get("company_name_raw") or raw_dict.get("company_name") or raw_dict.get("symbol")) or "Unknown Company"
    
    title, body = _generate_title_and_body(
        holder_name, company_name, tx_type, amount,
        holding_before, holding_after, purpose_en
    )

    tags = _normalize_tags(raw_dict.get("tags"), purpose_en)

    record = FilingRecord(
        symbol=_normalize_symbol(raw_dict.get("symbol") or raw_dict.get("issuer_code")),
        timestamp=_to_iso_date_full(main_date), # Pakai _full untuk timestamp utama
        transaction_type=tx_type,
        holder_name=holder_name,
        
        holding_before=holding_before,
        holding_after=holding_after,
        amount_transaction=amount,
        
        share_percentage_before=pp_before,
        share_percentage_after=pp_after,
        share_percentage_transaction=pp_tx,
        
        price=price,
        transaction_value=value,
        
        title=title,
        body=body,
        purpose_of_transaction=purpose_en,
        
        price_transaction=price_tx_list,
        tags=tags,
        
        sector=kebab(raw_dict.get("sector")),
        sub_sector=kebab(raw_dict.get("sub_sector")), 
        
        source=main_source_url, # Gunakan URL lengkap
        holder_type=_to_str(raw_dict.get("holder_type")),
        
        raw_data=raw_dict
    )
    
    return record

def transform_many(
    raw_dicts: List[Dict[str, Any]], 
    ingestion_map: Dict[str, Dict[str, Any]] # Tipe diubah ke Dict[str, Dict]
) -> List[FilingRecord]:
    """Helper function to transform a list of raw dicts."""
    clean_records = []
    for raw_dict in (raw_dicts or []):
        if not isinstance(raw_dict, dict):
            continue
        try:
            clean_records.append(transform_raw_to_record(raw_dict, ingestion_map))
        except Exception as e:
            logging.error(f"Failed to transform row: {e}. Row: {raw_dict}", exc_info=True)
            pass
    return clean_records