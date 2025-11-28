import os
from dataclasses import dataclass
from typing import Optional

# Logging
LOG_LEVEL = os.getenv("FILINGS_LOG_LEVEL", "INFO").upper()

# Legacy knobs (kept for backward compatibility)
# NOTE: PRICE_DEVIATION_THRESHOLD is retained for compatibility;
# the new Suspicious Price Detection uses separate thresholds.
PRICE_DEVIATION_THRESHOLD = float(os.getenv("FILINGS_PRICE_DEV_THRESHOLD", "0.7"))  # 50%
VALUE_KEEP_THRESHOLD = int(os.getenv("FILINGS_VALUE_KEEP_THRESHOLD", "100000000"))  # Rp100,000,000
PCT_KEEP_THRESHOLD = float(os.getenv("FILINGS_PCT_KEEP_THRESHOLD", "0.5"))          # 0.5%

# Data paths (hybrid)
COMPANY_MAP_PATH   = os.getenv("FILINGS_COMPANY_MAP",   "data/company/company_map.json")
LATEST_PRICES_PATH = os.getenv("FILINGS_LATEST_PRICES", "data/company/company_map.json")

# Fallback to a live source (e.g., Supabase) when the local cache misses
USE_LIVE_SUPABASE_ON_MISS = os.getenv("FILINGS_LIVE_ON_MISS", "0").lower() in ("1", "true", "yes")

# Suspicious price detection (flag & gate)
# Within-doc: compare transaction price vs the median price in the same document
WITHIN_DOC_RATIO_LOW  = float(os.getenv("FILINGS_WITHIN_DOC_RATIO_LOW",  "0.5"))  # < 0.5x median -> outlier
WITHIN_DOC_RATIO_HIGH = float(os.getenv("FILINGS_WITHIN_DOC_RATIO_HIGH", "1.5"))  # > 1.5x median -> outlier

# Market sanity: compare transaction price vs Close/VWAP over N days (default 20)
MARKET_REF_N_DAYS       = int(os.getenv("FILINGS_MARKET_REF_N_DAYS", "20"))
MARKET_RATIO_LOW        = float(os.getenv("FILINGS_MARKET_RATIO_LOW",  "0.6"))  # < 0.6x market ref -> outlier
MARKET_RATIO_HIGH       = float(os.getenv("FILINGS_MARKET_RATIO_HIGH", "1.4"))  # > 1.4x market ref -> outlier

# Magnitude anomaly: detect possible missing zeros (x10 / x100)
# If price is ~10x or 100x the reference (document median / market), flag possible_zero_missing=True
ZERO_MISSING_X10_MIN    = float(os.getenv("FILINGS_ZERO_MISSING_X10_MIN",   "8.0"))   # >= 8x considered an x10 candidate
ZERO_MISSING_X10_MAX    = float(os.getenv("FILINGS_ZERO_MISSING_X10_MAX",  "15.0"))   # <=15x
ZERO_MISSING_X100_MIN   = float(os.getenv("FILINGS_ZERO_MISSING_X100_MIN", "80.0"))   # >=80x
ZERO_MISSING_X100_MAX   = float(os.getenv("FILINGS_ZERO_MISSING_X100_MAX","150.0"))   # <=150x

# Suggested fair price range (for alert suggestions): ±ratio from reference
SUGGEST_PRICE_RATIO = float(os.getenv("FILINGS_SUGGEST_PRICE_RATIO", "0.15"))  # ±15%

# Recompute ownership % (model vs PDF -> flag, do not override)
# Tolerance gap expressed in percentage points (pp). For example, 0.25 means 0.25pp (not 25%).
PERCENT_TOL_PP = float(os.getenv("FILINGS_PERCENT_TOL_PP", "0.25"))  # default 0.25pp

# Alerts
# Directory & filename patterns for alerts v2 artifacts
ALERTS_OUTPUT_DIR = os.getenv("FILINGS_ALERTS_DIR", "artifacts")
ALERTS_INSERTED_FILENAME    = os.getenv("FILINGS_ALERTS_INSERTED_PATTERN",    "alerts_inserted_{date}.json")
ALERTS_NOT_INSERTED_FILENAME = os.getenv("FILINGS_ALERTS_NOT_INSERTED_PATTERN", "alerts_not_inserted_{date}.json")

# PLatest price sanity + gating email
# Maximum reasonable delta pp compared to expected (model)
DELTA_PP_MAX = float(os.getenv("FILINGS_DELTA_PP_MAX", "0.5"))  # 0.5pp

# Price freshness (days) for the latest price used in sanity checks
PRICE_LOOKBACK_DAYS = int(os.getenv("FILINGS_PRICE_LOOKBACK_DAYS", "14"))

# Gate Inserted emails when certain conditions occur:
# - stale_price, missing_price, delta_pp_mismatch, suspicious_price_level, percent_discrepancy, etc.
GATE_REASONS = set(
    s.strip() for s in os.getenv(
        "FILINGS_GATE_REASONS",
        # default list of reasons that block "Inserted" emails
        "suspicious_price_level, percent_discrepancy, stale_price, missing_price, delta_pp_mismatch"
    ).split(",")
    if s.strip()
)

# Email / SES
DEFAULT_AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1"
DEFAULT_ALERT_TO   = os.getenv("ALERT_TO_EMAIL",  "")
DEFAULT_ALERT_CC   = os.getenv("ALERT_CC_EMAIL",  "")
DEFAULT_ALERT_BCC  = os.getenv("ALERT_BCC_EMAIL", "")