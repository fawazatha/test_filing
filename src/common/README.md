# Common Module

Shared utilities used across ingestion, downloader, parser, transformer, generation, services, and workflow. Provides safe file I/O, logging, datetime helpers, proxy/env handling, string/number normalization, HTTP client setup, and Supabase REST helper.

## Directory Contents & Critical Functions
- `files.py`
  - Safe path helpers: `ensure_clean_dir`, `ensure_dir`, `ensure_parent`, `safe_unlink`, `safe_mkdirs`.
  - Atomic writers: `write_text`, `write_bytes`, `atomic_write_json`, `write_json`, `write_jsonl`.
  - Readers: `read_text`, `read_json` (tolerant returns None on failure).
  - Filename utility: `safe_filename_from_url(url, default="file.pdf")` (sanitizes/normalizes).
- `env.py`
  - `proxies_from_env()`: builds `{"http://": proxy, "https://": proxy}` from common env vars (`PROXY`, `HTTP[S]_PROXY`).
- `log.py`
  - `get_logger(name="app", level=logging.INFO)`: standardized console logger (formatter `[LEVEL] ts name: msg`).
- `datetime.py`
  - WIB timezone constant: `JAKARTA_TZ`.
  - Parsers/formatters: `parse_id_en_date`, `timestamp_jakarta`, `now_wib`, `iso_wib`, `iso_utc`, `fmt_wib_date`, `fmt_wib_range`.
- `strings.py`
  - Company key normalization: `normalize_company_key`, `normalize_company_key_lower`, `strip_diacritics`.
  - Slugs: `kebab`, `slugify`.
  - Safe conversions: `to_int`, `to_float`, `to_bool`, `normalize_space`.
  - Tokenization constants for corp stopwords/uppercase tokens.
- `numbers.py`
  - Tolerant parsing: `NumberParser.parse_number`, `NumberParser.parse_percentage`.
  - Math helpers: `safe_div`, `pct_close` (pp tolerance), `_floor_pct5`.
- `http.py`
  - `init_http(timeout=30.0, headers=None)`: httpx client factory.
  - `get_pdf_bytes_minimal(client, url)`: fetch PDF bytes with basic error handling.
- `sb.py`
  - Supabase REST helper (httpx): `fetch`, `fetch_all` with flexible filters (`filters`, `in_filters`, `eq/gte/...`, `ilike`), pagination, headers built from `SUPABASE_URL`/`SUPABASE_KEY`.

## Usage Patterns
- File I/O: use atomic writers for outputs (`atomic_write_json`) to avoid partial writes; `ensure_clean_dir` is guarded against dangerous paths.
- Proxies: call `proxies_from_env` or rely on clients (ingestion/downloader) that apply it automatically.
- Datetime: use `timestamp_jakarta`/`now_wib`/`iso_wib` for WIB consistency; `fmt_wib_range` for reporting windows.
- Strings/numbers: use provided parsers for mixed-locale numbers/percentages and company-name normalization to keep matching consistent.
- HTTP: prefer `http.init_http` when needing a plain httpx client; downloader uses its own requests client with UA/referer.
- Supabase: use `sb.fetch`/`fetch_all` for REST queries with filters and pagination; requires env `SUPABASE_URL`/`SUPABASE_KEY`.

## Environment
- Proxies: `PROXY`, `HTTP_PROXY`, `HTTPS_PROXY`, lowercase variants.
- Supabase: `SUPABASE_URL`, `SUPABASE_KEY`.

## Edge Cases / Behavior
- `ensure_clean_dir` refuses to operate on dangerous paths (`/`, HOME, CWD).
- `read_json` returns `None` on errors/missing file instead of raising.
- Number parsers tolerate mixed separators and percentages; invalid inputs return 0/0.0.
- Purposefully ASCII-safe slugging/diacritic stripping for consistency.

## Extend/Modify
- Add new safe file ops: extend `files.py` but keep guardrails.
- Add string/number normalization variants as needed by new data sources.
- Extend Supabase helper with additional operators if required by queries.
