import os

# Logging
LOG_LEVEL = os.getenv("FILINGS_LOG_LEVEL", "INFO").upper()

# Thresholds
PRICE_DEVIATION_THRESHOLD = float(os.getenv("FILINGS_PRICE_DEV_THRESHOLD", "0.5"))  # 50%
VALUE_KEEP_THRESHOLD = int(os.getenv("FILINGS_VALUE_KEEP_THRESHOLD", "100000000"))  # Rp100,000,000
PCT_KEEP_THRESHOLD = float(os.getenv("FILINGS_PCT_KEEP_THRESHOLD", "0.5"))  # 0.5%

# Cache paths (hybrid)
COMPANY_MAP_PATH      = os.getenv("FILINGS_COMPANY_MAP",      "data/company/company_map.json")
LATEST_PRICES_PATH    = os.getenv("FILINGS_LATEST_PRICES",    "data/company/latest_prices.json")

# Live fallback 
USE_LIVE_SUPABASE_ON_MISS = os.getenv("FILINGS_LIVE_ON_MISS", "0") in ("1", "true", "TRUE")
