import os

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
LOG_LEVEL = os.getenv("FILINGS_LOG_LEVEL", "INFO").upper()

# -----------------------------------------------------------------------------
# Legacy knobs (dipertahankan agar backward-compatible)
# -----------------------------------------------------------------------------
# NOTE: PRICE_DEVIATION_THRESHOLD lama dipertahankan untuk kompatibilitas;
# fitur Suspicious Price Detection yang baru menggunakan threshold terpisah.
PRICE_DEVIATION_THRESHOLD = float(os.getenv("FILINGS_PRICE_DEV_THRESHOLD", "0.7"))  # 50%
VALUE_KEEP_THRESHOLD = int(os.getenv("FILINGS_VALUE_KEEP_THRESHOLD", "100000000"))  # Rp100,000,000
PCT_KEEP_THRESHOLD = float(os.getenv("FILINGS_PCT_KEEP_THRESHOLD", "0.5"))          # 0.5%

# -----------------------------------------------------------------------------
# Data paths (hybrid)
# -----------------------------------------------------------------------------
COMPANY_MAP_PATH   = os.getenv("FILINGS_COMPANY_MAP",   "data/company/company_map.json")
LATEST_PRICES_PATH = os.getenv("FILINGS_LATEST_PRICES", "data/company/company_map.json")

# Fallback ke sumber live (mis. Supabase) kalau local cache miss
USE_LIVE_SUPABASE_ON_MISS = os.getenv("FILINGS_LIVE_ON_MISS", "0").lower() in ("1", "true", "yes")

# -----------------------------------------------------------------------------
# P0-4 • Suspicious price detection (flag & gate)
# -----------------------------------------------------------------------------
# Within-doc: bandingkan harga transaksi vs median harga di dokumen yang sama
WITHIN_DOC_RATIO_LOW  = float(os.getenv("FILINGS_WITHIN_DOC_RATIO_LOW",  "0.5"))  # < 0.5x median → outlier
WITHIN_DOC_RATIO_HIGH = float(os.getenv("FILINGS_WITHIN_DOC_RATIO_HIGH", "1.5"))  # > 1.5x median → outlier

# Market sanity: bandingkan harga transaksi vs Close/VWAP N-day (default 20)
MARKET_REF_N_DAYS       = int(os.getenv("FILINGS_MARKET_REF_N_DAYS", "20"))
MARKET_RATIO_LOW        = float(os.getenv("FILINGS_MARKET_RATIO_LOW",  "0.6"))  # < 0.6x market ref → outlier
MARKET_RATIO_HIGH       = float(os.getenv("FILINGS_MARKET_RATIO_HIGH", "1.4"))  # > 1.4x market ref → outlier

# Magnitude anomaly: deteksi kemungkinan "nol hilang" (x10 / x100)
# Jika harga ≈ 10x atau 100x referensi (median dokumen / market), flag possible_zero_missing=True
ZERO_MISSING_X10_MIN    = float(os.getenv("FILINGS_ZERO_MISSING_X10_MIN",   "8.0"))   # >= 8x dianggap kandidat x10
ZERO_MISSING_X10_MAX    = float(os.getenv("FILINGS_ZERO_MISSING_X10_MAX",  "15.0"))   # <=15x
ZERO_MISSING_X100_MIN   = float(os.getenv("FILINGS_ZERO_MISSING_X100_MIN", "80.0"))   # >=80x
ZERO_MISSING_X100_MAX   = float(os.getenv("FILINGS_ZERO_MISSING_X100_MAX","150.0"))   # <=150x

# Saran rentang harga wajar (untuk suggestion di alerts): ±ratio dari referensi
SUGGEST_PRICE_RATIO = float(os.getenv("FILINGS_SUGGEST_PRICE_RATIO", "0.15"))  # ±15%

# -----------------------------------------------------------------------------
# P0-5 • Recompute ownership % (model vs PDF → flag, tidak override)
# -----------------------------------------------------------------------------
# Toleransi selisih dalam satuan "percentage points (pp)". Misal 0.25 berarti 0.25pp (bukan 25%).
PERCENT_TOL_PP = float(os.getenv("FILINGS_PERCENT_TOL_PP", "0.25"))  # default 0.25pp

# -----------------------------------------------------------------------------
# P0-A • Alerts v2 (unified IDX & non-IDX; Inserted & Not Inserted; + Mapping)
# -----------------------------------------------------------------------------
# Direktori & pola nama file untuk artifacts alerts v2
ALERTS_OUTPUT_DIR = os.getenv("FILINGS_ALERTS_DIR", "artifacts")
ALERTS_INSERTED_FILENAME    = os.getenv("FILINGS_ALERTS_INSERTED_PATTERN",    "alerts_inserted_{date}.json")
ALERTS_NOT_INSERTED_FILENAME = os.getenv("FILINGS_ALERTS_NOT_INSERTED_PATTERN", "alerts_not_inserted_{date}.json")

# -----------------------------------------------------------------------------
# P1-6 • Latest price sanity + gating email
# -----------------------------------------------------------------------------
# Batas maksimum delta pp "yang masuk akal" dibanding expected (model)
DELTA_PP_MAX = float(os.getenv("FILINGS_DELTA_PP_MAX", "0.5"))  # 0.5pp

# Freshness harga (hari) untuk latest price yang dipakai sanity check
PRICE_LOOKBACK_DAYS = int(os.getenv("FILINGS_PRICE_LOOKBACK_DAYS", "14"))

# Gate email (Inserted) bila terjadi kondisi tertentu:
# - stale_price, missing_price, delta_pp_mismatch, suspicious_price_level, percent_discrepancy, dll.
# (Implementasi gating ada di send_alerts.py, variabel ini sekadar daftar "reason" yang mem-gate)
GATE_REASONS = set(
    s.strip() for s in os.getenv(
        "FILINGS_GATE_REASONS",
        # default daftar reasons yang memblok email "Inserted"
        "suspicious_price_level, percent_discrepancy, stale_price, missing_price, delta_pp_mismatch"
    ).split(",")
    if s.strip()
)

# -----------------------------------------------------------------------------
# Email / SES
# -----------------------------------------------------------------------------
DEFAULT_AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1"
DEFAULT_ALERT_TO   = os.getenv("ALERT_TO_EMAIL",  "")
DEFAULT_ALERT_CC   = os.getenv("ALERT_CC_EMAIL",  "")
DEFAULT_ALERT_BCC  = os.getenv("ALERT_BCC_EMAIL", "")
