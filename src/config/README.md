# Config Module

Centralizes pipeline knobs and thresholds, primarily for filings validation, alerts gating, paths, and email defaults. Values are loaded from environment variables with sane defaults.

## File
- `config.py` — constants read throughout generation/upload stages (and some parser/generator helpers).

## Key Settings (env overrides)
- Logging: `LOG_LEVEL` (default `INFO`).
- Legacy thresholds (compat): `PRICE_DEVIATION_THRESHOLD`, `VALUE_KEEP_THRESHOLD`, `PCT_KEEP_THRESHOLD`.
- Paths: `FILINGS_COMPANY_MAP` (default `data/company/company_map.json`), `FILINGS_LATEST_PRICES` (default same path), `FILINGS_LIVE_ON_MISS` (1/0) to hit live Supabase on cache miss.
- Suspicious price detection:
  - Within document: `FILINGS_WITHIN_DOC_RATIO_LOW` (0.5), `FILINGS_WITHIN_DOC_RATIO_HIGH` (1.5).
  - Market sanity: `FILINGS_MARKET_REF_N_DAYS` (20), `FILINGS_MARKET_RATIO_LOW` (0.6), `FILINGS_MARKET_RATIO_HIGH` (1.4).
  - Zero-missing heuristics: `FILINGS_ZERO_MISSING_X10_MIN/MAX` (8.0/15.0), `FILINGS_ZERO_MISSING_X100_MIN/MAX` (80.0/150.0).
  - Suggested price band: `FILINGS_SUGGEST_PRICE_RATIO` (0.15).
- Percentage tolerance: `FILINGS_PERCENT_TOL_PP` (0.25pp).
- Alerts output patterns: `FILINGS_ALERTS_DIR` (default `artifacts`), `FILINGS_ALERTS_INSERTED_PATTERN` (`alerts_inserted_{date}.json`), `FILINGS_ALERTS_NOT_INSERTED_PATTERN` (`alerts_not_inserted_{date}.json`).
- Price sanity + gating email: `FILINGS_DELTA_PP_MAX` (0.5pp), `FILINGS_PRICE_LOOKBACK_DAYS` (14).
- Email gating reasons: `FILINGS_GATE_REASONS` (comma list; defaults include `suspicious_price_level`, `percent_discrepancy`, `stale_price`, `missing_price`, `delta_pp_mismatch`).
- AWS/Email defaults: `AWS_REGION`/`AWS_DEFAULT_REGION` (default `ap-southeast-1`), `ALERT_TO_EMAIL`, `ALERT_CC_EMAIL`, `ALERT_BCC_EMAIL`.

## Usage
- Import constants directly: `from src.config.config import LOG_LEVEL, FILINGS_WITHIN_DOC_RATIO_LOW, ...`
- Typical consumers:
  - `generate/filings` processors for price/percent checks and alert gating.
  - Upload/email paths for alerts/artifacts.
  - Parser/generator helpers may read log level defaults.

## Extending
- Add new knobs to `config.py` with env overrides; keep defaults reasonable.
- Keep names consistent (`FILINGS_*`) for clarity and grep-ability.
- Document new settings here and ensure downstream modules read them instead of hardcoding.
