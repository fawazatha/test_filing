from __future__ import annotations
from typing import List, Dict, Optional, Tuple

import logging

logger = logging.getLogger(__name__)


class TransactionClassifier:
    @staticmethod
    def classify_transaction_type(
        text: str,
        pct_before: float,
        pct_after: float
    ) -> Tuple[str, List[str]]:
 
        tl = (text or "").lower()

        if any(k in tl for k in ["perbaikan", "koreksi", "ralat", "errata", "amendment"]):
            return "correction", ["correction", "insider_trading"]

        # Heuristik crossing 50%
        def crosses_majority(a: float, b: float) -> bool:
            try:
                return (a < 50 <= b) or (a >= 50 > b)
            except Exception:
                return False

        is_takeover = crosses_majority(pct_before, pct_after)

        sell_kw = ["jual", "penjualan", "sell", "divestasi", "divestment", "pengurangan", "reduksi", "disposal"]
        buy_kw  = ["beli", "pembelian", "buy", "akumulasi", "investasi", "acquisition", "penambahan", "increase", "buyback", "buy back"]
        neutral_kw = ["transfer", "pemindahan", "konversi", "conversion", "hibah", "waris", "neutral", "tanpa perubahan"]

        if any(k in tl for k in sell_kw):
            tags = ["bearish", "ownership_change", "insider_trading"]
            if is_takeover: tags.append("takeover")
            return "sell", tags

        if any(k in tl for k in buy_kw):
            tags = ["bullish", "ownership_change", "insider_trading"]
            if is_takeover: tags.append("takeover")
            return "buy", tags

        if any(k in tl for k in neutral_kw):
            tags = ["neutral", "ownership_change", "insider_trading"]
            if is_takeover: tags.append("takeover")
            return "neutral", tags

        try:
            if float(pct_after) > float(pct_before):
                tags = ["bullish", "ownership_change", "insider_trading"]
                if is_takeover: tags.append("takeover")
                return "buy", tags
            if float(pct_after) < float(pct_before):
                tags = ["bearish", "ownership_change", "insider_trading"]
                if is_takeover: tags.append("takeover")
                return "sell", tags
        except Exception:
            pass

        return "neutral", ["neutral", "ownership_change", "insider_trading"]

    @staticmethod
    def infer_direction(
        holding_before: Optional[int],
        holding_after: Optional[int],
        pct_before: Optional[float],
        pct_after: Optional[float]
    ) -> str:
        """
        Infer arah ('buy'/'sell'/'neutral') dari perubahan holdings/persentase.
        """
        try:
            if isinstance(holding_before, (int, float)) and isinstance(holding_after, (int, float)):
                if holding_after > holding_before:
                    return "buy"
                if holding_after < holding_before:
                    return "sell"
        except Exception:
            pass

        try:
            if isinstance(pct_before, (int, float)) and isinstance(pct_after, (int, float)):
                if pct_after > pct_before:
                    return "buy"
                if pct_after < pct_before:
                    return "sell"
        except Exception:
            pass

        return "neutral"

    @staticmethod
    def mismatch_flag(
        doc_type: Optional[str],
        inferred: str,
        holding_before: Optional[int],
        holding_after: Optional[int],
        pct_before: Optional[float],
        pct_after: Optional[float]
    ) -> Optional[Dict]:

        doc = (doc_type or "").strip().lower()
        if doc in {"buy", "sell"} and inferred in {"buy", "sell"} and doc != inferred:
            return {
                "type": "Transaction Type Mismatch",
                "message": f"Document says '{doc}', but holdings/percentages imply '{inferred}'.",
                "document_type": doc,
                "inferred_type": inferred,
                "holding_before": holding_before,
                "holding_after": holding_after,
                "share_percentage_before": pct_before,
                "share_percentage_after": pct_after,
            }
        return None
