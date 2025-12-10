# Downloader Module

Classifies announcements as IDX vs Non-IDX, downloads PDFs, records metadata, and emits download-stage alerts. It is the bridge between ingestion (announcements JSON) and parsing (PDF inputs).

## Directory Contents
- `cli.py` — CLI entrypoint to run downloads with flags for output paths, retries, similarity threshold, verbosity, and clean-up.
- `runner.py` — core workflow: classify announcements, build URL lists, download PDFs with retries, write metadata/alerts.
- `client.py` — HTTP helpers (requests-based) with proxy/env handling and referer seeding.
- `utils/classifier.py` — fuzzy title classifier for IDX vs Non-IDX and low-title-similarity checks.
- `utils/announcement.py` — Pydantic model describing announcement shape.

## Data Flow
1) **Input**: normalized announcements JSON from ingestion (`data/ingestion.json` by default).
2) **Classify**: `utils/classifier.classify_format(title, threshold)` → label `IDX` / `NON-IDX` / `UNKNOWN` with fuzzy scores. `UNKNOWN` → low-similarity alert.
3) **URL extraction**:
   - IDX → `announcement.main_link`
   - Non-IDX → all attachment URLs (from `Announcement.attachments`)
   - Deduplicate URLs.
4) **Download** (`runner._download_with_retries`):
   - First attempt: `client.get_pdf_bytes_minimal` (UA + referer).
   - Retries: `client.seed_and_retry_minimal` (hit referer to seed cookies, then retry).
   - Save PDFs to `downloads/idx-format` or `downloads/non-idx-format`.
5) **Record metadata**: `data/downloaded_pdfs.json` with ticker/title/url/filename/timestamp.
6) **Alerts**:
   - `low_title_similarity` (skip download for UNKNOWN).
   - `download_failed` (all retries failed).
   - Alerts written to `alerts/alerts_not_inserted_downloader.json` (and staged per v2 schema).
7) **Outputs**: PDFs, metadata JSON, alerts JSON.

## CLI Usage (`python -m src.downloader.cli`)
```bash
python -m src.downloader.cli \
  --input data/ingestion.json \
  --out-idx downloads/idx-format \
  --out-non-idx downloads/non-idx-format \
  --meta-out data/downloaded_pdfs.json \
  --alerts-out alerts/low_title_similarity_alerts.json \
  [--retries 3] [--min-similarity 80] [--dry-run] [--verbose] [--clean-out]
```
Flags:
- `--retries`: total attempts per URL (1 minimal + retries-1 seeded).
- `--min-similarity`: fuzzy threshold to decide IDX vs Non-IDX; below = UNKNOWN.
- `--dry-run`: no downloads; only log/record metadata skeleton.
- `--clean-out`: wipe output folders/files before run (safe guards in place).

## Critical Functions / Responsibilities
- `runner.download_pdfs(announcements, out_idx, out_non_idx, meta_out, alerts_out, ...)`
  - Prepares outputs, cleans if requested, initializes HTTP, builds ingestion index for context, loops announcements, classifies, downloads, records metadata, builds alerts.
  - Writes alerts into v2 buckets: `alerts/alerts_inserted_downloader.json`, `alerts/alerts_not_inserted_downloader.json`.
- `_download_with_retries(url, out_path, retries, logger)`
  - Single minimal GET, then seeded retries; returns success bool.
- `_attachment_to_url(att)` / `_derive_ticker(title, company_name)`
  - URL extraction and fallback ticker derivation from title patterns.
- `client.init_http(...)`
  - Set env proxies from `PROXY`, silence SSL warnings if desired.
- `client.get_pdf_bytes_minimal(url)` / `client.seed_and_retry_minimal(url)`
  - Requests with UA + referer to satisfy IDX servers.
- `utils/classifier`
  - `classify_format(title, threshold)`: fuzzy classify between known IDX vs Non-IDX patterns.
  - `low_title_similarity(title, filename, threshold=0.35)`: jaccard check to flag mismatched title/filename.
- `utils/announcement.Announcement`
  - Pydantic model used to validate/structure input announcements.

## Inputs & Outputs (contract)
- Input: announcements JSON array shaped by ingestion (fields: `title`, `main_link`, `attachments`, `date`, etc.).
- Outputs:
  - PDFs saved to `downloads/idx-format` and `downloads/non-idx-format`.
  - Metadata JSON: `data/downloaded_pdfs.json`.
  - Alerts: `alerts/alerts_not_inserted_downloader.json` (plus legacy `alerts_out` path).

## Environment & Config
- Proxies: `PROXY`, `HTTP_PROXY`, `HTTPS_PROXY` (auto-applied in `client.init_http`).
- HTTP headers: set in `client.py` (UA + referer).
- Logging: uses `src.common.log.get_logger`; `--verbose` to lower log level.

## Edge Cases & Behavior
- No URLs in announcement → skip with warning (no alert unless low-similarity).
- `UNKNOWN` classification (fuzzy score < threshold) → skip download, emit `low_title_similarity` alert.
- Corrupt/unsupported PDFs → `download_failed` after retries.
- `--clean-out` safely recreates output dirs/files (guarded against dangerous paths).

## Extend/Modify
- New classification patterns: update `utils/classifier.py` known phrases or thresholds.
- Different download strategy (e.g., headless browser): replace `_download_with_retries`/`client` logic.
- Extra metadata fields: adjust `records` payload in `runner.download_pdfs`.
- Additional alert codes: use `services.alert.schema.build_alert` in `runner` to add new conditions.
