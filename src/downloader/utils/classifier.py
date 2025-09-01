from typing import Tuple
from rapidfuzz import fuzz

IDX_KNOWN = [
    "Ownership Report or Any Changes in Ownership of Public Company Shares",
]
NON_IDX_KNOWN = [
    "Share Ownership Report",
    "Laporan Kepemilikan Saham",
]

def classify_format(title: str, threshold: int = 80) -> Tuple[str, int, int, int]:
    """
    Return (label, best_score, idx_score, non_idx_score)
      - label ∈ {"IDX", "NON-IDX", "UNKNOWN"}
      - best_score = max(idx_score, non_idx_score)
    """
    t = (title or "").strip().lower()
    idx_score = max((fuzz.token_set_ratio(t, k.lower()) for k in IDX_KNOWN), default=0)
    non_score = max((fuzz.token_set_ratio(t, k.lower()) for k in NON_IDX_KNOWN), default=0)
    best = max(idx_score, non_score)

    if best < threshold:
        return "UNKNOWN", best, idx_score, non_score
    label = "IDX" if idx_score >= non_score else "NON-IDX"
    return label, best, idx_score, non_score
