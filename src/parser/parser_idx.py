from __future__ import annotations
from typing import List, Dict, Optional, Tuple, Any
import os
import re

# --- Core/Base ---
from src.parser.core.base_parser import BaseParser

# --- Common Libs ---
from src.common.numbers import NumberParser
from src.common.log import get_logger

# --- Parser Utils ---
from src.parser.utils.text_extractor import TextExtractor
from src.parser.utils.transaction_extractor import TransactionExtractor
from src.parser.utils.name_cleaner import NameCleaner
from src.parser.utils.transaction_classifier import TransactionClassifier
from src.parser.utils.company import (
    CompanyService,
    pretty_company_name,
    suggest_symbols
)


logger = get_logger(__name__)

SYMBOL_TOKEN_RE = re.compile(r"^[A-Z0-9]{3,6}$")


class IDXParser(BaseParser):
    """
    IDX-format parser (English page).
    Refactored to:
    - Use common NumberParser directly
    - Use centralized TransactionClassifier.validate_direction
    - Use centralized TransactionExtractor for parsing rows (DRY)
    - Use CompanyService facade for all resolution
    """

    def __init__(
        self,
        pdf_folder: str = "downloads/idx-format",
        output_file: str = "data/parsed_idx_output.json",
        announcement_json: str = "data/idx_announcements.json",
    ):
        super().__init__(
            pdf_folder,
            output_file,
            announcement_json,
            alerts_file=os.getenv("ALERTS_IDX", "alerts/alerts_idx.json"),
            alerts_not_inserted_file=os.getenv("ALERTS_NOT_INSERTED_IDX", "alerts/alerts_not_inserted_idx.json"),
        )
        self._current_alert_context: Optional[Dict[str, Any]] = None
        
        # Centralized service for all company logic
        self.company = CompanyService()
        self.company_map = self.company.symbol_to_name
        self._rev_company_map = self.company.rev_map
        self.company_names = set(self.company_map.values())

    # == Entry point ==
    def parse_single_pdf(
        self, filepath: str, filename: str, pdf_mapping: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        
        self._current_alert_context = (pdf_mapping or {}).get(filename) or {}

        text = self.extract_text_from_pdf(filepath)
        if not text:
            self.alert_manager.log_alert(filename, "no_text_extracted", {
                "announcement": self._current_alert_context
            })
            return None

        text = self._slice_to_english(text)
        self.save_debug_output(filename, text)

        try:
            data = self.extract_fields_from_text(text, filename)
            data["source"] = filename

            flags = TransactionClassifier.detect_flags_from_text(text)
            txns = (data.get("transactions") or [])
            
            # Synthesize txns if empty but type is known
            if not txns and data.get("transaction_type") in {"buy", "sell", "transfer"}:
                txns = [{"type": data["transaction_type"], "amount": data.get("amount_transacted") or 0}]

            data["tags"] = TransactionClassifier.compute_filings_tags(
                txns=txns,
                share_percentage_before=data.get("share_percentage_before"),
                share_percentage_after=data.get("share_percentage_after"),
                flags=flags,
            )
            return data
            
        except Exception as e:
            logger.error(f"extract_fields error {filename}: {e}", exc_info=True)
            self.alert_manager.log_alert(filename, f"field_extract_error: {e}", {
                "announcement": self._current_alert_context
            })
            return None

    # == Slicing ==
    def _slice_to_english(self, text: str) -> str:
        lines = (text or "").splitlines()
        for i, ln in enumerate(lines):
            if "go to indonesian page" in (ln or "").lower():
                return "\n".join(lines[i + 1:])
        return text

    # == Field extraction ==
    def extract_fields_from_text(self, text: str, filename: str) -> Dict[str, Any]:
        ex = TextExtractor(text)
        res: Dict[str, Any] = {"lang": "en"}

        self._extract_headers(ex, res)
        self._resolve_issuer(ex, res, filename)
        self._extract_holder(ex, res, filename)
        if res.get("skip_filing"):  # early exit
            return res

        self._extract_holdings_and_percentages(ex, res)
        self._extract_contact(ex, res)
        
        # --- FIX: Reverted to logic from your original working file ---
        self._extract_purpose(ex, res)
        # --- End FIX ---

        self._extract_transactions(ex, res)
        self._postprocess_transactions(res)

        tx_type = res.get("transaction_type")
        if tx_type in ("buy", "sell"):
            ok, reason = TransactionClassifier.validate_direction(
                res.get("share_percentage_before"),
                res.get("share_percentage_after"),
                tx_type
            )
            if not ok:
                logger.warning("Skipping inconsistent %s: %s", tx_type, reason)
                res["skip_filing"] = True
                res["skip_reason"] = reason
                res.setdefault("parse_warnings", []).append(reason)
                return res

        self._flag_type_mismatch_if_any(res, filename)
        return res

    # ---- smaller helpers ----
    def _extract_headers(self, ex: TextExtractor, res: Dict[str, Any]) -> None:
        res["issuer_code"] = (
            ex.find_table_value("Issuer Name")
            or ex.find_value_in_line("Issuer Name")
            or ""
        ).strip()
        res["attachments"] = (
            ex.find_table_value("Listing Board")
            or ex.find_value_in_line("Listing Board")
            or ""
        ).strip()
        res["subject"] = (
            ex.find_table_value("Attachments")
            or ex.find_value_in_line("Attachments")
            or ""
        ).strip()

    def _resolve_issuer(self, ex: TextExtractor, res: Dict[str, Any], filename: str) -> None:
        issuer_name_raw = (
            ex.find_table_value("Name of Share of Public Company")
            or ex.find_value_in_line("Name of Share of Public Company")
            or ""
        ).strip()

        res["company_name_raw"] = issuer_name_raw or ""
        res["symbol"] = None
        company_name_out: str = issuer_name_raw
        sym: Optional[str] = None

        if issuer_name_raw:
            token = issuer_name_raw.strip().upper()
            
            if SYMBOL_TOKEN_RE.fullmatch(token) and (
                token in self.company_map or f"{token}.JK" in self.company_map
            ):
                sym = token if token in self.company_map else f"{token}.JK"
                if not sym.endswith(".JK"):
                    sym = f"{sym}.JK"
                company_name_out = self.company.get_canonical_name(sym) or issuer_name_raw
            
            if not sym:
                min_score = int(os.getenv("COMPANY_RESOLVE_MIN_SCORE", "85"))
                base = self.company.resolve_symbol(issuer_name_raw, issuer_name_raw, min_score_env=str(min_score))
                if base:
                    sym = base if base.endswith(".JK") else f"{base}.JK"
                    company_name_out = self.company.get_canonical_name(sym) or issuer_name_raw

        if issuer_name_raw and not sym:
            norm_key = self.company.normalized_key(issuer_name_raw)
            suggestions = suggest_symbols(
                issuer_name_raw,
                self.company_map,
                self._rev_company_map,
                top_k=int(os.getenv("COMPANY_SUGGEST_TOPK", "3")),
            )
            self.alert_manager_not_inserted.log_alert(
                filename, "Symbol Not Resolved from Name",
                {
                    "company_name_raw": issuer_name_raw,
                    "normalized_key": norm_key,
                    "suggestions": suggestions,
                    "announcement": self._current_alert_context,
                },
            )
            res["skip_filing"] = True
            res["skip_reason"] = "Symbol Not Resolved from Name"
            res.setdefault("parse_warnings", []).append("Symbol Not Resolved from Name")
            company_name_out = pretty_company_name(issuer_name_raw)

        res["company_name"] = company_name_out or ""
        res["symbol"] = sym or None

    def _extract_holder(self, ex: TextExtractor, res: Dict[str, Any], filename: str) -> None:
        holder_name_raw = (
            ex.find_table_value("Name of Shareholder")
            or ex.find_value_in_line("Name of Shareholder")
            or ""
        ).strip()
        res["holder_name_raw"] = holder_name_raw

        holder_type = NameCleaner.classify_holder_type(holder_name_raw)
        res["holder_type"] = holder_type

        if holder_type == "institution":
            hsym, disp, _, _ = self._resolve_holder_institution(holder_name_raw)
            res["holder_name"] = disp
            res["holder_symbol"] = hsym
        else:
            res["holder_name"] = NameCleaner.clean_holder_name(holder_name_raw, "insider")
            res["holder_symbol"] = None

        if not NameCleaner.is_valid_holder(res.get("holder_name")):
            res["skip_filing"] = True
            res["skip_reason"] = "Invalid holder_name"
            res.setdefault("parse_warnings", []).append("Invalid holder_name")

    def _resolve_holder_institution(self, raw: str) -> Tuple[Optional[str], str, str, List[str]]:
        return self.company.resolve_symbol_and_name(
            raw,
            fuzzy=True,
            min_score=int(os.getenv("COMPANY_RESOLVE_MIN_SCORE", "80")),
        )

    def _extract_holdings_and_percentages(self, ex: TextExtractor, res: Dict[str, Any]) -> None:
        res["holding_before"] = NumberParser.parse_number(
            ex.find_number_after_keyword("Number of shares owned before the transaction")
        )
        res["holding_after"] = NumberParser.parse_number(
            ex.find_number_after_keyword("Number of shares owned after the transaction")
        )
        res["share_percentage_before"] = NumberParser.parse_percentage(
            ex.find_percentage_after_keyword("Percentage of ownership before the transaction")
        )
        res["share_percentage_after"] = NumberParser.parse_percentage(
            ex.find_percentage_after_keyword("Percentage of ownership after the transaction")
        )
        res["share_percentage_transaction"] = abs(
            (res.get("share_percentage_after") or 0.0) - (res.get("share_percentage_before") or 0.0)
        )

    def _extract_contact(self, ex: TextExtractor, res: Dict[str, Any]) -> None:
        addr = (
            ex.find_value_in_line("Address")
            or ex.find_value_after_keyword("Address")
            or ""
        ).strip()
        if not addr:
            for ln in ex.lines or []:
                L = (ln or "").strip().lower()
                if L.startswith(("graha", "gedung", "tower", "jl", "jalan")):
                    addr = ln.strip()
                    break
        if addr:
            res["company_address"] = addr

        phone = (
            ex.find_value_in_line("Telephone Number")
            or ex.find_value_after_keyword("Telephone Number")
            or ""
        ).strip()
        if phone:
            res["company_phone"] = phone

    # --- FIX: Logic updated to match your working file ---
    def _extract_purpose(self, ex: TextExtractor, res: Dict[str, Any]) -> None:
        """Extracts the purpose of the transaction text."""
        purpose = (
            ex.find_table_value("Purposes of transaction")
            or ex.find_value_in_line("Purposes of transaction")
            # Add singular and fallbacks for robustness
            or ex.find_table_value("Purpose of transaction")
            or ex.find_value_in_line("Purpose of transaction")
            or ex.find_value_after_keyword("Purposes of transaction")
            or ex.find_value_after_keyword("Purpose of transaction")
            or ""
        ).strip()
            
        res["purpose"] = purpose
    # --- End FIX ---

    # == Transactions ==
    def _extract_transactions(self, ex: TextExtractor, res: Dict[str, Any]) -> None:
        for i, line in enumerate(ex.lines or []):
            if "transaction type" in (line or "").lower():
                for j in range(i + 1, min(i + 8, len(ex.lines))):
                    t = (ex.lines[j] or "").lower()
                    if "buy" in t:
                        res["transaction_type"] = "buy"; break
                    if "sell" in t:
                        res["transaction_type"] = "sell"; break
                    if "transfer" in t:
                        res["transaction_type"] = "transfer"; break
                break

        tx_ex = TransactionExtractor(ex, ticker=res.get("symbol"))
        rows = tx_ex.extract_transaction_rows()
        
        if not rows and (res.get("transaction_type") == "transfer" or tx_ex.contains_transfer_transaction()):
             rows = tx_ex.extract_transfer_transactions()

        res["transactions"] = rows

    # == Post-processing & validation ==
    def _postprocess_transactions(self, res: Dict[str, Any]) -> None:
        txs = res.get("transactions") or []
        buy_sell = [t for t in txs if t.get("type") in {"buy", "sell"}]
        transfers = [t for t in txs if t.get("type") == "transfer"]

        rows_amt = sum(t.get("amount", 0) for t in buy_sell)
        rows_val = sum(t.get("value", 0.0) for t in buy_sell)

        hb = res.get("holding_before")
        ha = res.get("holding_after")
        delta_amt = None
        try:
            if isinstance(hb, (int, float)) and isinstance(ha, (int, float)):
                delta_amt = abs(int(ha) - int(hb))
        except Exception:
            delta_amt = None

        res["amount_transacted_rows"] = rows_amt
        res["amount_transacted"] = (delta_amt if (delta_amt is not None) else rows_amt)
        res["transaction_value"] = rows_val

        res["has_transfer"] = bool(transfers)
        res["amount_transferred"] = sum(t.get("amount", 0) for t in transfers)
        res["value_transferred"] = sum(t.get("value", 0.0) for t in transfers)

        if not res.get("transaction_type"):
            kinds = {t.get("type") for t in txs}
            if kinds == {"transfer"}:
                res["transaction_type"] = "transfer"
            elif kinds <= {"buy", "sell"} and len({t["type"] for t in buy_sell}) == 1:
                res["transaction_type"] = buy_sell[0]["type"]

        total_amt = sum(t.get("amount", 0) for t in buy_sell if t.get("amount", 0) > 0)
        if total_amt > 0:
            wavg = sum((t.get("price", 0.0) * t.get("amount", 0)) for t in buy_sell) / total_amt
            res["price"] = round(wavg, 2)
        else:
            res["price"] = None

        res["price_transaction"] = {
            "prices": [t.get("price", 0.0) for t in buy_sell if t.get("amount", 0) > 0],
            "amount_transacted": [t.get("amount", 0) for t in buy_sell if t.get("amount", 0) > 0],
        }

        res["is_transfer"] = res.get("is_transfer", False) or res["has_transfer"]

    def _flag_type_mismatch_if_any(self, res: Dict[str, Any], filename: str) -> None:
        doc_type = (res.get("transaction_type") or "").lower()
        inferred = TransactionClassifier.infer_direction(
            holding_before=res.get("holding_before", 0),
            holding_after=res.get("holding_after", 0),
            pct_before=res.get("share_percentage_before", 0.0),
            pct_after=res.get("share_percentage_after", 0.0),
        )
        
        mismatch = TransactionClassifier.mismatch_flag(
            doc_type,
            inferred,
            res.get("holding_before"),
            res.get("holding_after"),
            res.get("share_percentage_before"),
            res.get("share_percentage_after"),
        )
        
        if mismatch:
            hb = res.get("holding_before")
            ha = res.get("holding_after")
            pb = res.get("share_percentage_before")
            pa = res.get("share_percentage_after")

            self.alert_manager.log_alert(
                filename,
                "Transaction Type Mismatch",
                {
                    "symbol": res.get("symbol"),
                    "company_name": res.get("company_name"),
                    "document_type": doc_type,
                    "inferred_type": inferred,
                    "holding_before": hb,
                    "holding_after": ha,
                    "share_percentage_before": pb,
                    "share_percentage_after": pa,
                    "announcement": self._current_alert_context,
                },
            )

    def validate_parsed_data(self, d: Dict[str, Any]) -> bool:
        if d.get("skip_filing"):
            return False
            
        all_zero = (
            not d.get("holder_name")
            and (d.get("holding_before", 0) == 0)
            and (d.get("holding_after", 0) == 0)
            and (d.get("share_percentage_before", 0.0) == 0.0)
            and (d.get("share_percentage_after", 0.0) == 0.0)
        )
        has_change = (d.get("holding_before") != d.get("holding_after")) or \
                     (d.get("share_percentage_before") != d.get("share_percentage_after"))
        
        has_txns = bool(d.get("transactions"))
        
        return not all_zero and (has_change or has_txns)

