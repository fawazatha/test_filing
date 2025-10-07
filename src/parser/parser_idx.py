import os
import re
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

from .core.base_parser import BaseParser
from .utils.text_extractor import TextExtractor
from .utils.number_parser import NumberParser
from .utils.name_cleaner import NameCleaner
from .utils.transaction_classifier import TransactionClassifier
from .utils.company_resolver import (
    build_reverse_map,
    resolve_symbol_from_emiten,
    canonical_name_for_symbol,
    normalize_company_name,
    suggest_symbols,
    resolve_symbol_and_name,   # <-- NEW
    pretty_company_name,       # <-- NEW
)

logger = logging.getLogger(__name__)

EN_DATE_PATTERN = (
    r"(?:\d{1,2})\s+"
    r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+"
    r"\d{4}"
)

SYMBOL_TOKEN_RE = re.compile(r"^[A-Z0-9]{3,6}$")


# -------------------------------
# Helper: validasi arah transaksi
# -------------------------------
def _validate_tx_direction(
    before: Optional[float],
    after: Optional[float],
    tx_type: str,
    eps: float = 1e-3
) -> Tuple[bool, Optional[str]]:
    """
    Direction sanity:
      - buy  => after >= before (±eps)
      - sell => after <= before (±eps)
    """
    try:
        b = float(before) if before is not None else None
        a = float(after) if after is not None else None
    except Exception:
        return False, "non_numeric_before_after"
    if b is None or a is None:
        return False, "missing_before_or_after"

    t = (tx_type or "").strip().lower()
    if t == "buy" and a + eps < b:
        return False, f"inconsistent_buy: after({a}) < before({b})"
    if t == "sell" and a > b + eps:
        return False, f"inconsistent_sell: after({a}) > before({b})"
    return True, None


class IDXParser(BaseParser):
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

        self.company_map = self._load_company_mapping() or self.symbol_to_name or {}
        self._rev_company_map = build_reverse_map(self.company_map)
        self.company_names = set(self.company_map.values())

    def _load_company_mapping(self) -> Dict[str, Any]:
        """Load local company map and normalize keys:
           - dukung struktur dict maupun list
           - simpan versi base (ABCD) dan full (ABCD.JK) -> nama
        """
        try:
            import json
            path = os.getenv("COMPANY_MAP_FILE", "data/company/company_map.json")
            if not os.path.exists(path):
                logger.warning(f"Company mapping not found: {path}")
                return {}

            raw = json.loads(Path(path).read_text(encoding="utf-8"))
            out: Dict[str, Any] = {}

            def add(sym: str, nm: Optional[str]):
                if not sym or not nm:
                    return
                s = str(sym).strip().upper()
                n = str(nm).strip()
                if not s or not n:
                    return
                # simpan keduanya (full & base)
                if s.endswith(".JK"):
                    out[s] = n
                    out[s[:-3]] = n
                else:
                    out[s] = n
                    out[f"{s}.JK"] = n

            if isinstance(raw, dict):
                for k, v in raw.items():
                    if isinstance(v, dict):
                        add(k, v.get("company_name") or v.get("name") or v.get("legal_name"))
                    elif isinstance(v, str):
                        add(k, v)
            elif isinstance(raw, list):
                for item in raw:
                    add(item.get("symbol", ""), item.get("company_name", ""))
            else:
                logger.error(f"Unsupported company_map.json structure: {type(raw).__name__}")
                return {}

            logger.info(f"Loaded {len(out)} company symbols from local mapping")
            return out

        except Exception as e:
            logger.error(f"load company_map error: {e}")
            return {}

    # Entry points
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
            return data
        except Exception as e:
            logger.error(f"extract_fields error {filename}: {e}")
            self.alert_manager.log_alert(filename, f"field_extract_error: {e}", {
                "announcement": self._current_alert_context
            })
            return None

    def _slice_to_english(self, text: str) -> str:
        lines = (text or "").splitlines()
        for i, ln in enumerate(lines):
            if "go to indonesian page" in (ln or "").lower():
                return "\n".join(lines[i + 1:])
        return text

    def extract_fields_from_text(self, text: str, filename: str) -> Dict[str, Any]:
        ex = TextExtractor(text)
        res: Dict[str, Any] = {"lang": "en"}

        # Header-ish fields (sesuai pola dokumen IDX EN)
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

        issuer_name_raw = (
            ex.find_table_value("Name of Share of Public Company")
            or ex.find_value_in_line("Name of Share of Public Company")
            or ""
        ).strip()

        sym: Optional[str] = None
        company_name_out: str = issuer_name_raw

        if issuer_name_raw:
            token = issuer_name_raw.strip().upper()

            # Case A: token adalah ticker dan ada di mapping → pakai sebagai simbol
            if SYMBOL_TOKEN_RE.fullmatch(token) and (
                token in self.company_map or f"{token}.JK" in self.company_map
            ):
                sym = token if token in self.company_map else f"{token}.JK"
                if not sym.endswith(".JK"):
                    sym = f"{sym}.JK"

                company_name_out = (
                    canonical_name_for_symbol(self.company_map, sym) or issuer_name_raw
                )

            # Case B: bukan ticker → resolve dari nama emiten (fuzzy)
            if not sym:
                min_score = int(os.getenv("COMPANY_RESOLVE_MIN_SCORE", "85"))
                sym2, _k, _t = resolve_symbol_from_emiten(
                    issuer_name_raw,
                    symbol_to_name=self.company_map,
                    rev_map=self._rev_company_map,
                    fuzzy=True,
                    min_score=min_score,
                )
                if sym2:
                    sym2 = sym2.upper()
                    if not sym2.endswith(".JK"):
                        sym2 = f"{sym2}.JK"
                    sym = sym2
                    company_name_out = (
                        canonical_name_for_symbol(self.company_map, sym) or issuer_name_raw
                    )

        # Jika tetap tidak dapat symbol → alert + tandai skip_filing
        if issuer_name_raw and not sym:
            norm_key = normalize_company_name(issuer_name_raw)
            suggestions = suggest_symbols(
                issuer_name_raw,
                self.company_map,
                self._rev_company_map,
                top_k=int(os.getenv("COMPANY_SUGGEST_TOPK", "3")),
            )

            self.alert_manager_not_inserted.log_alert(
                filename,
                "Symbol Not Resolved from Name",
                {
                    "company_name_raw": issuer_name_raw,
                    "normalized_key": norm_key,
                    "missing_in_company_map": norm_key not in (self._rev_company_map or {}),
                    "suggestions": suggestions,
                    "announcement": self._current_alert_context,
                },
            )

            res["skip_filing"] = True
            res["skip_reason"] = "Symbol Not Resolved from Name"
            res.setdefault("parse_warnings", []).append("Symbol Not Resolved from Name")

            # tetap tampilkan nama perusahaan rapih
            company_name_out = pretty_company_name(issuer_name_raw)

        # Persist company fields
        res["company_name_raw"] = issuer_name_raw or ""
        res["company_name"] = company_name_out or ""
        res["symbol"] = sym or None

        # Toleransi typo "Controling"
        res["classification_of_shareholder"] = (
            ex.find_table_value("Classification of Shareholder")
            or ex.find_value_in_line("Classification of Shareholder")
            or ""
        ).strip()

        res["controlling_shareholder"] = (
            ex.find_table_value("Controlling Shareholder")
            or ex.find_value_in_line("Controlling Shareholder")
            or ex.find_table_value("Controling Shareholder")
            or ex.find_value_in_line("Controling Shareholder")
            or ""
        ).strip()

        res["citizenship"] = (
            ex.find_table_value("Citizenship")
            or ex.find_value_in_line("Citizenship")
            or ""
        ).strip()

        res["percentage_of_shares_traded"] = NumberParser.parse_percentage(
            ex.find_table_value("Percentage of Shares traded")
            or ex.find_value_in_line("Percentage of Shares traded")
        )

        res["share_ownership_status"] = (
            ex.find_table_value("Share Ownership Status")
            or ex.find_value_in_line("Share Ownership Status")
            or ""
        ).strip()

        res["purpose"] = (
            ex.find_table_value("Purposes of transaction")
            or ex.find_value_in_line("Purposes of transaction")
            or ""
        ).strip()

        # ---------------------------
        # Holder (DROP-IN UPDATED)
        # ---------------------------
        holder_name_raw = (
            ex.find_table_value("Name of Shareholder")
            or ex.find_value_in_line("Name of Shareholder")
            or ""
        ).strip()

        res["holder_name_raw"] = holder_name_raw

        holder_type = NameCleaner.classify_holder_type(holder_name_raw)
        res["holder_type"] = holder_type

        if holder_type == "institution":
            # Coba resolve simbol; jika gagal, tetap kembalikan display name yang rapih
            hsym, disp, _key, _tried = resolve_symbol_and_name(
                holder_name_raw,
                self.company_map,
                rev_map=self._rev_company_map,
                fuzzy=True,
                min_score=int(os.getenv("COMPANY_RESOLVE_MIN_SCORE", "80")),
            )
            res["holder_name"] = disp
            res["holder_symbol"] = hsym  # bisa None
        else:
            # Insider (perorangan): bersihkan nama, tanpa simbol
            res["holder_name"] = NameCleaner.clean_holder_name(holder_name_raw, "insider")
            res["holder_symbol"] = None

        # +++ VALIDASI HOLDER NAME
        if not NameCleaner.is_valid_holder(res.get("holder_name")):
            res["skip_filing"] = True
            res["skip_reason"] = "Invalid holder_name"
            res.setdefault("parse_warnings", []).append("Invalid holder_name")
            return res  # atau return None, sesuai preferensi kamu

        # ---------------------------
        # Holdings / percentages
        # ---------------------------
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

        # Address (fallback heuristik)
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

        # Transactions parse
        self._extract_transactions_en(ex, res)
        self._postprocess_transactions(res)

        # +++ VALIDASI ARAH TRANSAKSI (butuh res["transaction_type"] sudah ada)
        tx_type = res.get("transaction_type")
        if tx_type in ("buy", "sell"):
            ok, reason = _validate_tx_direction(
                res.get("share_percentage_before"),
                res.get("share_percentage_after"),
                tx_type
            )
            if not ok:
                logger.warning("Skipping inconsistent %s: %s", tx_type, reason)
                res["skip_filing"] = True
                res["skip_reason"] = reason
                res.setdefault("parse_warnings", []).append(reason)
                return res  # atau return None

        # Mismatch alert (tetap kirim untuk observability)
        self._flag_type_mismatch_if_any(res, filename)
        return res

    # in IDXParser._extract_transactions_en
    def _extract_transactions_en(self, ex: TextExtractor, res: Dict[str, Any]) -> None:
        # Doc-level declared type (termasuk transfer)
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

        full_text = "\n".join(ex.lines or [])
        rows = self._parse_transactions_text_en(full_text)
        if not rows:
            rows = self._parse_transactions_lines_en(ex.lines or [])

        res["transactions"] = rows

    def _parse_transactions_text_en(self, text: str) -> List[Dict[str, Any]]:
        if not text:
            return []
        pat = re.compile(
            rf"Type of Transaction:\s*(?P<typ>Buy|Sell|Transfer)\s*.*?"
            rf"Transaction Price:\s*(?P<price>[\d\.,]+)\s*.*?"
            rf"Transaction Date:\s*(?P<date>{EN_DATE_PATTERN})\s*.*?"
            rf"Number of Shares Transacted:\s*(?P<amount>[\d\.,]+)",
            flags=re.I | re.S,
        )
        out: List[Dict[str, Any]] = []
        for m in pat.finditer(text):
            typ_raw = (m.group("typ") or "").strip().lower()
            if typ_raw.startswith("b"):
                typ = "buy"
            elif typ_raw.startswith("s"):
                typ = "sell"
            else:
                typ = "transfer"
            price = NumberParser.parse_number(m.group("price")) or 0.0
            amt = NumberParser.parse_number(m.group("amount")) or 0
            out.append({
                "type": typ,
                "price": price,
                "amount": amt,
                "date": m.group("date"),
                "value": price * amt,
            })
        return out

    def _parse_transactions_lines_en(self, lines: List[str]) -> List[Dict[str, Any]]:
        if not lines:
            return []
        row_re = re.compile(
            rf"\b(?P<typ>Buy|Sell|Transfer)\b\s+(?P<price>[\d\.,]+)\s+(?P<date>{EN_DATE_PATTERN})\s+(?P<amount>[\d\.,]+)",
            flags=re.I,
        )
        out: List[Dict[str, Any]] = []
        for raw in lines:
            m = row_re.search(raw or "")
            if not m:
                continue
            typ_raw = (m.group("typ") or "").lower()
            if typ_raw.startswith("b"):
                typ = "buy"
            elif typ_raw.startswith("s"):
                typ = "sell"
            else:
                typ = "transfer"
            price = NumberParser.parse_number(m.group("price")) or 0.0
            amt = NumberParser.parse_number(m.group("amount")) or 0
            out.append({
                "type": typ,
                "price": price,
                "amount": amt,
                "date": m.group("date"),
                "value": price * amt,
            })
        return out

    def _postprocess_transactions(self, res: Dict[str, Any]) -> None:
        txs = res.get("transactions") or []

        buy_sell = [t for t in txs if t.get("type") in {"buy", "sell"}]
        transfers = [t for t in txs if t.get("type") == "transfer"]

        # --- Totals (rows vs holdings delta) ---
        rows_amt = sum(t.get("amount", 0) for t in buy_sell)             # hanya BUY/SELL
        rows_val = sum(t.get("value", 0.0) for t in buy_sell)

        # Delta holdings (utamakan ini untuk amount_transacted)
        hb = res.get("holding_before")
        ha = res.get("holding_after")
        delta_amt = None
        try:
            if isinstance(hb, (int, float)) and isinstance(ha, (int, float)):
                delta_amt = abs(int(ha) - int(hb))
        except Exception:
            delta_amt = None

        # Set field utama
        res["amount_transacted_rows"] = rows_amt                    # referensi/debug
        res["amount_transacted"] = (delta_amt if (delta_amt is not None) else rows_amt)
        res["transaction_value"] = rows_val                         # nilai dari baris BUY/SELL (tetap)

        # Transfer-only metrics (tetap dipisah)
        res["has_transfer"] = bool(transfers)
        res["amount_transferred"] = sum(t.get("amount", 0) for t in transfers)
        res["value_transferred"] = sum(t.get("value", 0.0) for t in transfers)

        # Doc-level type jika belum ada
        if not res.get("transaction_type"):
            kinds = {t.get("type") for t in txs}
            if kinds == {"transfer"}:
                res["transaction_type"] = "transfer"
            elif kinds <= {"buy", "sell"} and len({t["type"] for t in buy_sell}) == 1:
                res["transaction_type"] = buy_sell[0]["type"]

        # Weighted average price untuk BUY/SELL saja (tidak untuk transfer)
        total_amt = sum(t.get("amount", 0) for t in buy_sell if t.get("amount", 0) > 0)
        if total_amt:
            wavg = sum((t.get("price", 0.0) * t.get("amount", 0)) for t in buy_sell) / total_amt
            res["price"] = round(wavg, 2)

        # Simpan rincian price_transaction (BUY/SELL only)
        res["price_transaction"] = {
            "prices": [t.get("price", 0.0) for t in buy_sell if t.get("amount", 0) > 0],
            "amount_transacted": [t.get("amount", 0) for t in buy_sell if t.get("amount", 0) > 0],
        }

        # Tanda transfer
        res["is_transfer"] = res.get("is_transfer", False) or res["has_transfer"]

    def _flag_type_mismatch_if_any(self, res: Dict[str, Any], filename: str) -> None:
        doc_type = (res.get("transaction_type") or "").lower()
        inferred = TransactionClassifier.infer_direction(
            holding_before=res.get("holding_before", 0),
            holding_after=res.get("holding_after", 0),
            pct_before=res.get("share_percentage_before", 0.0),
            pct_after=res.get("share_percentage_after", 0.0),
        )
        flag = TransactionClassifier.mismatch_flag(
            doc_type,
            inferred,
            res.get("holding_before"),
            res.get("holding_after"),
            res.get("share_percentage_before"),
            res.get("share_percentage_after"),
        )
        if flag:
            hb = res.get("holding_before")
            ha = res.get("holding_after")
            pb = res.get("share_percentage_before")
            pa = res.get("share_percentage_after")

            delta_h: Optional[float] = None
            try:
                if isinstance(hb, (int, float)) and isinstance(ha, (int, float)):
                    delta_h = ha - hb
            except Exception:
                pass

            delta_p: Optional[float] = None
            try:
                if isinstance(pb, (int, float)) and isinstance(pa, (int, float)):
                    delta_p = round(pa - pb, 6)
            except Exception:
                pass

            bits: List[str] = []
            if delta_h is not None:
                bits.append(f"holdings d={delta_h:+}")
            if delta_p is not None:
                bits.append(f"share% d={delta_p:+}")
            why = f"{'; '.join(bits)} → implies '{inferred}'" if bits else f"data implies '{inferred}'"

            transfer_hint = bool(
                (isinstance(hb, (int, float)) and hb == 0)
                or (isinstance(ha, (int, float)) and ha == 0)
            )

            self.alert_manager.log_alert(
                filename,
                "Transaction Type Mismatch",
                {
                    "symbol": res.get("symbol"),
                    "company_name": res.get("company_name"),
                    "document_type": flag["document_type"],
                    "inferred_type": flag["inferred_type"],
                    "explanation": why,
                    "holding_before": hb,
                    "holding_after": ha,
                    "share_percentage_before": pb,
                    "share_percentage_after": pa,
                    "is_transfer_hint": transfer_hint,
                    "announcement": self._current_alert_context,
                },
            )

    def _alert_symbol_mismatch(self, filename, raw, canon, sym_from_name, sym_doc):
        self.alert_manager.log_alert(filename, "symbol_name_mismatch", {
            "company_name_raw": raw,
            "company_name_canonical": canon,
            "symbol_from_name": sym_from_name,
            "symbol_in_doc": sym_doc,
            "announcement": self._current_alert_context,
        })

    def validate_parsed_data(self, d: Dict[str, Any]) -> bool:
        all_zero = (
            not d.get("holder_name")
            and (d.get("holding_before", 0) == 0)
            and (d.get("holding_after", 0) == 0)
            and (d.get("share_percentage_before", 0.0) == 0.0)
            and (d.get("share_percentage_after", 0.0) == 0.0)
        )
        return not all_zero and bool(d.get("transactions"))
