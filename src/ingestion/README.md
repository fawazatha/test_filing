# Ingestion Module

Fetch IDX “Ownership Report” announcements and normalize them into a stable JSON shape for downstream downloader/parser stages. Time handling is WIB-aware and supports single-day, date ranges, month ranges, and hour spans.

## Directory Contents
- `cli.py` — CLI entrypoint for fetching announcements in different modes.
- `runner.py` — orchestration helpers (fetch loops, dedupe, save).
- `client.py` — httpx client configured for IDX API + env proxies.
- `utils/config.py` — constants (IDX API URL, headers, page size).
- `utils/filters.py` — date parsing, validation, WIB window computation.
- `utils/normalizer.py` — normalize raw IDX items into the pipeline schema.
- `utils/sorters.py` — publish-time sorting helpers.

## Data Flow
1) **Fetch** via `client.fetch_page()` using IDX API parameters (`keywords=ownership`, `dateFrom`, `dateTo`, `pageNumber`, `pageSize`).  
2) **Filter** by publish timestamp window (WIB) if provided.  
3) **Normalize** each raw item → normalized dict with title, code, main link, attachments, filename, etc. (`utils/normalizer.py`).  
4) **Deduplicate** on `(id)` or `(link,title,date)`.  
5) **Save** to JSON with UTF-8 + indentation (`runner.save_json`).

Default output path: `data/ingestion.json`.

## CLI Usage (`python -m src.ingestion.cli`)
- **Single day (optional HH:MM window)**  
  `--date YYYYMMDD [--start-hhmm HH:MM --end-hhmm HH:MM]`
- **Date range (inclusive, WIB)**  
  `--from-date YYYYMMDD --to-date YYYYMMDD`
- **Month**  
  `--month YYYYMM or YYYY-MM`
- **Hour span across dates**  
  `--start YYYYMMDD HOUR --end YYYYMMDD HOUR` (end hour inclusive)
- Common flags:  
  - `--sort asc|desc` (by publish time; default desc)  
  - `--out data/ingestion.json` (output path)  
  - `--env-file .env` (load proxies, etc.)

Examples:
```bash
# Single day, full-day window
python -m src.ingestion.cli --date 20250115 --out data/ingestion.json

# Single day, time window 09:00–17:00 WIB
python -m src.ingestion.cli --date 20250115 --start-hhmm 09:00 --end-hhmm 17:00

# Range
python -m src.ingestion.cli --from-date 20250110 --to-date 20250115

# Month
python -m src.ingestion.cli --month 2025-01

# Hour span (cross-day allowed, end hour inclusive)
python -m src.ingestion.cli --start 20250114 20 --end 20250115 10
```

## Outputs (normalized schema)
Each item resembles:
```json
{
  "date": "2025-01-15T10:23:45",
  "title": "Ownership Report or Any Changes in Ownership of Public Company Shares",
  "title_slug": "ownership-report-or-any-changes-in-ownership-of-public-company-shares",
  "company_name": "BBRI",
  "main_link": "https://www.idx.co.id/...",
  "filename": "ownership-report-bbri.pdf",
  "attachments": [
    {"filename": "attachment.pdf", "url": "https://..."}
  ],
  "attachment_count": 1,
  "category": "Ownership Report",
  "description": "Ownership report for BBRI",
  "link": "https://www.idx.co.id/...",
  "scraped_at": "2025-01-15T17:00:00+07:00"
}
```
Notes:
- `attachments` keeps only extras (index 1..n); `main_link` is attachment[0].
- `filename` is a sanitized basename derived from `main_link`.
- Items without valid attachments are dropped.

## Key Internals
- `runner.get_ownership_announcements_range(start_yyyymmdd, end_yyyymmdd, start_dt=None, end_dt=None)`  
  Core loop: paginate per day, filter by WIB window, normalize, dedupe.
- `runner.get_ownership_announcements(date_yyyymmdd, start_hhmm=None, end_hhmm=None)`  
  Single-day convenience wrapper; uses `compute_range_and_window`.
- `runner.get_ownership_announcements_span(start_yyyymmdd, start_hour, end_yyyymmdd, end_hour)`  
  Hour-precision span (end hour inclusive).
- `runner.save_json(data, out_path)`  
  Writes UTF-8 with indent=2.
- `client.make_client(timeout=60.0, transport=None)`  
  Configured httpx client; applies env proxies via `common.env.proxies_from_env`; disables SSL verify for compatibility.
- `client.fetch_page(client, start_yyyymmdd, end_yyyymmdd, page, page_size=DEFAULT_PAGE_SIZE)`  
  Calls IDX API with `keywords=ownership`.
- `utils/filters.py`  
  - `parse_publish_wib(str) -> datetime` (supports ISO with/without TZ, coerces to WIB).  
  - `compute_range_and_window(date, start_hhmm, end_hhmm)` (handles cross-midnight).  
  - `compute_month_range(year_month)` → `(YYYYMMDD_start, YYYYMMDD_end)`.  
  - `compute_span_from_date_hour(...)` → `(date_from, date_to, start_dt, end_dt)`.  
  - `in_window(dt, start_dt, end_dt)` inclusive bounds.  
  - `validate_yyyymmdd`.
- `utils/normalizer.normalize_item(item)`  
  Shapes raw IDX payload into the schema; drops rows with no valid attachments.
- `utils/sorters.sort_announcements(items, order="desc")`  
  Sort by publish time (WIB), then title, then link.

## Configuration & Environment
- IDX API URL / headers / page size: `utils/config.py`.
- Proxies: set `PROXY` or `HTTP(S)_PROXY`; `client.py` maps them to env for httpx.
- Logging: via `src.common.log.get_logger` in `runner.py`; CLI uses `get_logger("ingestion.cli")`.

## Contracts & Edge Cases
- All date strings must be `YYYYMMDD`; validation is strict.
- Time windows are WIB (UTC+7). Cross-midnight windows are supported (end date auto-extended).
- Deduping is by `id` if present; otherwise `(main_link|link, title, date)`.
- Normalizer requires at least one valid attachment URL; otherwise the row is skipped.
- Output is stable, UTF-8, indented JSON to ease diffing and downstream parsing.

## How to Extend
- Add new fetch filters: extend `runner` to accept extra CLI flags and pass them to `client.fetch_page`.
- Adjust sorting or dedupe: update `utils/sorters.py` or `_dedupe` in `runner.py`.
- Support alternative sources: add new client/normalizer modules and branch in `runner`.
- Capture more raw fields: extend `normalize_item` to copy them into the output schema.

## Common Issues
- **Empty results**: check date format (`YYYYMMDD`), window bounds, and IDX API availability.
- **Proxy/network**: ensure `PROXY`/`HTTP(S)_PROXY` set; SSL verify is disabled by default for compatibility.
- **Missing attachments**: the API sometimes returns items without valid `FullSavePath`; these are intentionally skipped.
