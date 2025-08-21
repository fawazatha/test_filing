from typing import Tuple
from thefuzz import fuzz

# Canonicals aligned with the working script
IDX_CANONICAL = "Ownership Report or Any Changes in Ownership of Public Company Shares"
NON_IDX_VARIANTS = [
    "Share Ownership Report",
    "Laporan Kepemilikan Saham",
]

def classify_format(title: str, threshold: int = 80) -> Tuple[str, int, int, int]:
    """
    Returns (label, best_score, sim_idx, sim_non)
      label ∈ {"IDX", "NON-IDX", "UNKNOWN"}

    Rule:
      - compute sim_idx (vs IDX_CANONICAL) and sim_non (max vs NON_IDX_VARIANTS)
      - pick the label with the higher score IF it meets threshold
      - otherwise, return UNKNOWN
    """
    t = (title or "").lower()
    sim_idx = fuzz.token_set_ratio(t, IDX_CANONICAL.lower())
    sim_non = max(fuzz.token_set_ratio(t, v.lower()) for v in NON_IDX_VARIANTS)

    if sim_idx >= sim_non and sim_idx >= threshold:
        return "IDX", sim_idx, sim_idx, sim_non
    if sim_non > sim_idx and sim_non >= threshold:
        return "NON-IDX", sim_non, sim_idx, sim_non
    return "UNKNOWN", max(sim_idx, sim_non), sim_idx, sim_non
