# Scripts

Ad-hoc/utility scripts that support company map management and data exports from Supabase. These are standalone helpers, not part of the main ingestion/download/parse/generate pipeline.

## Files
- `company_map_hybrid.py` — Build/refresh local company map (`data/company/company_map.json`) from Supabase `idx_company_report`.
- `company_report.py` — Export rows from `idx_company_report` to JSON/CSV with optional filters.
- `fetch_filings.py` — Fetch rows from Supabase `idx_filings` over a flexible time window.

## company_map_hybrid.py
Purpose: maintain a single-source company map cache with sector/sub-sector and latest price info.
- Inputs: Supabase REST (`SUPABASE_URL`, `SUPABASE_KEY`/`SERVICE_ROLE`/`ANON`), table `idx_company_report` (configurable via env), optional offline cache.
- Outputs: `data/company/company_map.json` (normalized symbols, company_name, sector, sub_sector, price, latest_close_date) and `data/company/company_map.meta.json` (metadata/checksum).
- Key functions:
  - `_build_url`, `_headers`: Supabase REST helpers with schema support.
  - `load_local` / `save_local`: read/write local cache + meta.
  - `remote_row_count`, `fetch_remote`: pull rows with paging.
  - `_normalize_full`, `_normalize_sector`, `_checksum`: normalization and integrity hash.
  - Flags: `COMPANY_MAP_ALLOW_OFFLINE` (allow using local when remote unreachable), `COMPANY_MAP_FORCE_REFRESH` (force remote fetch).
- Usage:
  ```bash
  python -m src.scripts.company_map_hybrid  # adjust PYTHONPATH or run from repo root
  ```

## company_report.py
Purpose: export `idx_company_report` as JSON/CSV with optional symbol/tags filters.
- Inputs: Supabase REST (`SUPABASE_URL`, `SUPABASE_KEY`, optional `COMPANY_SCHEMA`).
- Flags: `--select`, `--all`, `--symbol`, `--tags`, `--since-report`, `--until-report`, `--limit`, `--order`, `--out-json`, `--out-csv`, `--log-level`.
- Key functions:
  - `Cfg`: dataclass for Supabase connection/table/column names.
  - `_build_url`, `_headers`, `_paged_get`: REST GET with paging.
  - `export_company_report(...)`: fetch + client-side filters (symbols, tags, report-date).
  - `write_json`, `write_csv`: output writers.
- Usage:
  ```bash
  python -m src.scripts.company_report --all --out-json data/company_report.json
  python -m src.scripts.company_report --symbol BBRI.JK,BBCA.JK --out-csv data/report.csv
  ```

## fetch_filings.py
Purpose: pull `idx_filings` rows from Supabase for given time windows and filters.
- Inputs: Supabase REST (`SUPABASE_URL`, `SUPABASE_KEY`), table `idx_filings`.
- Capabilities:
  - Time window filters (`timestamp` with `gt/lt/gte/lte`), symbol/tags filters, ordering.
  - Pagination via range headers.
- Key functions:
  - `_build_query_params`: assemble PostgREST filters (supports `eq/gte/lte/gt/lt/ilike/in`).
  - `_rest_get` / `_rest_get_all`: REST GET with headers/Range and pagination.
  - Timestamp helpers: `_parse_dt_iso`, `_now_jkt`, `_to_utc_z`, `_fmt_for_ts_kind`.
  - CLI parser builds filters and formats output (JSON).
- Usage:
  ```bash
  python -m src.scripts.fetch_filings --since 2024-12-01 --until 2024-12-31 --symbol BBRI.JK
  ```

## Environment (common)
- `SUPABASE_URL`, `SUPABASE_KEY` (or `SUPABASE_SERVICE_ROLE_KEY`/`SUPABASE_ANON_KEY`).
- Optional: `COMPANY_SCHEMA`, `COMPANY_REPORT_TABLE`, `COMPANY_MAP_ALLOW_OFFLINE`, `COMPANY_MAP_FORCE_REFRESH`.

## Notes
- Scripts use requests/httpx directly (not the shared `common/sb.py`); ensure .env is loaded or env vars are set.
- Run from repo root or ensure `PYTHONPATH` includes `src` for absolute imports.
