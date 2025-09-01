from __future__ import annotations

from typing import List, Dict, Any, Union, Tuple, Optional
from pathlib import Path
import json

# -------- JSON writer --------

def write_filings_json(
    filings: List[Dict[str, Any]],
    out_path: Union[str, Path],
    ensure_ascii: bool = False,
    indent: int = 2,
) -> Path:
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)

    def _to_plain(obj: Any) -> Dict[str, Any]:
        if isinstance(obj, dict):
            return obj
        data: Dict[str, Any] = {}
        for k in dir(obj):
            if k.startswith("_"):
                continue
            try:
                v = getattr(obj, k)
            except Exception:
                continue
            if callable(v):
                continue
            data[k] = v
        return data

    payload = [_to_plain(f) for f in filings]
    p.write_text(
        json.dumps(payload, ensure_ascii=ensure_ascii, indent=indent, default=str),
        encoding="utf-8",
    )
    return p


# -------- UID scenarios post-pass --------

_CORP_TOKENS = {
    "PT", "PT.", "P.T", "TBK", "TBK.", "PERSEROAN", "TERBATAS",
    "LIMITED", "LTD", "LTD.", "COMPANY", "CO", "CO.", "(TBK", "TBK)"
}

def _normalize_name(s: Optional[str]) -> str:
    if not s:
        return ""
    x = s.upper()
    for ch in ",.;:()[]{}'\"/\\|-_":
        x = x.replace(ch, " ")
    toks = [t for t in x.split() if t and t not in _CORP_TOKENS]
    return " ".join(toks)

def _same_company(a: str, b: str) -> bool:
    na, nb = _normalize_name(a), _normalize_name(b)
    return na == nb and na != ""

def apply_uid_scenarios(
    filings: List[Dict[str, Any]],
    company_lookup: Optional[Dict[str, str]] = None,
    uid_window: int = 3,
) -> None:
    """In-place Scenario 1/2/3 application with ±uid_window pairing.

    Scenario 1: Matched transfer UID on same symbol (within window) → both set transaction_value=0, price=None, add tag 'transfer'.
    Scenario 2: No partner; holder looks like a person → treasury transfer with same zeroing rules and UI hint from share% delta.
    Scenario 3: If matched pair exists and holder_name == company_name(symbol) → suppress (trader), else tag intercorporate-[buy|sell].
    """
    n = len(filings)
    lookup = dict(company_lookup or {})

    # Build index (symbol, transfer_uid) -> positions
    xfer_idx: Dict[Tuple[str, str], List[int]] = {}
    for i, f in enumerate(filings):
        sym = (f.get("symbol") or "").upper().strip()
        for t in f.get("transactions") or []:
            if t.get("type") == "transfer" and t.get("transfer_uid"):
                key = (sym, t["transfer_uid"])
                xfer_idx.setdefault(key, []).append(i)

    def _apply_transfer_zero(f: Dict[str, Any]) -> None:
        f["transaction_value"] = 0.0
        f["price"] = None
        f.setdefault("tags", [])
        if "transfer" not in f["tags"]:
            f["tags"].append("transfer")

    def _in_window(center: int, cand: int) -> bool:
        return abs(cand - center) <= uid_window

    def _looks_person(name: str) -> bool:
        raw = (name or "").upper()
        return not any(tok in raw for tok in _CORP_TOKENS)

    for i, f in enumerate(filings):
        sym = (f.get("symbol") or "").upper().strip()
        holder = f.get("holder_name") or ""
        direction = f.get("transaction_type") or f.get("direction")
        p_before = float(f.get("share_percentage_before") or 0.0)
        p_after = float(f.get("share_percentage_after") or 0.0)

        uids = [t.get("transfer_uid") for t in (f.get("transactions") or []) if t.get("type") == "transfer" and t.get("transfer_uid")]
        if not uids:
            continue

        comp_name = lookup.get(sym, "") or ""

        for uid in uids:
            positions = xfer_idx.get((sym, uid), [])
            partner_idx = None
            for j in positions:
                if j == i:
                    continue
                if _in_window(i, j):
                    partner_idx = j
                    break

            if partner_idx is not None:
                # Scenario 1 baseline
                partner = filings[partner_idx]
                _apply_transfer_zero(f)
                _apply_transfer_zero(partner)

                # Scenario 3 tagging/suppress for this filing
                if _same_company(holder, comp_name):
                    f.setdefault("extra", {})
                    f["extra"]["suppress"] = True
                    f["extra"]["scenario"] = "trader"
                else:
                    tag = "intercorporate-buy" if direction == "buy" else "intercorporate-sell"
                    if tag not in f["tags"]:
                        f["tags"].append(tag)

                # Partner side
                ph = partner.get("holder_name") or ""
                pd = partner.get("transaction_type") or partner.get("direction")
                p_comp = lookup.get((partner.get("symbol") or "").upper().strip(), "") or ""
                if _same_company(ph, p_comp):
                    partner.setdefault("extra", {})
                    partner["extra"]["suppress"] = True
                    partner["extra"]["scenario"] = "trader"
                else:
                    p_tag = "intercorporate-buy" if pd == "buy" else "intercorporate-sell"
                    partner.setdefault("tags", [])
                    if p_tag not in partner["tags"]:
                        partner["tags"].append(p_tag)
            else:
                # Scenario 2
                if _looks_person(holder):
                    _apply_transfer_zero(f)
                    f.setdefault("extra", {})
                    f["extra"]["scenario"] = "treasury_transfer"
                    if p_after < p_before:
                        f["extra"]["ui_card"] = {"left": "holder", "right": "treasury"}
                    elif p_after > p_before:
                        f["extra"]["ui_card"] = {"left": "treasury", "right": "holder"}
