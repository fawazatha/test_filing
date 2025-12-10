# Sectors IDX Filing Pipeline

Pipeline for harvesting IDX ownership announcements, downloading source PDFs, parsing filings, enriching/normalizing them, uploading to Supabase, and routing alerts/notifications. Paths are workspace-relative (`downloads/`, `data/`, `alerts/`, `artifacts/`) and all stages can be run independently or chained.

## Architecture
- **Ingestion (`src/ingestion`)** – fetch IDX “Ownership Report” announcements into normalized JSON.
- **Downloader (`src/downloader`)** – classify IDX vs Non-IDX, download PDFs, emit download metadata and low-similarity/download-fail alerts.
- **Parser (`src/parser`)** – extract structured fields and transaction rows from PDFs (IDX + Non-IDX variants) with parser-stage alerts.
- **Core/Transformer (`src/core`)** – map parsed dicts to canonical `FilingRecord`, enrich sector/sub-sector, translate purposes, tag directionality.
- **Generation/Upload (`src/generate/filings`)** – consolidate parsed outputs, run price/percent sanity checks, deduplicate, upload to Supabase, produce alert files/artifacts, optionally email.
- **Workflow (`src/workflow`)** – read workflow defs + MV rows from Supabase, build events, and dispatch to Slack/Email/Sheets/WhatsApp channels.
- **Orchestrator (`src/pipeline/orchestrator.py`)** – optional glue to chain the above for scheduled runs (clean outputs, ensure company map, generate artifacts/emails).
- **Services (`src/services`)** – shared alert schema, upload helpers, email/WhatsApp/Sheets senders, artifacts.

## Quick Start
Prereqs: Python 3.10+, `pip install -r requirements.txt`. Set `.env` for proxies and Supabase/SES as needed.

1. Fetch announcements (WIB-aware):
   ```bash
   python -m src.ingestion.cli --date 20250115 --out data/ingestion.json
   ```
2. Download PDFs + metadata/alerts:
   ```bash
   python -m src.downloader.cli --input data/ingestion.json \
     --out-idx downloads/idx-format --out-non-idx downloads/non-idx-format \
     --meta-out data/downloaded_pdfs.json
   ```
3. Parse PDFs (both formats):
   ```bash
   python -m src.parser.cli --parser both --announcements data/ingestion.json \
     --idx-folder downloads/idx-format --non-idx-folder downloads/non-idx-format
   ```
4. Transform + upload filings (alerts/artifacts/email):
   ```bash
   python -m src.generate.filings.cli \
     --parsed-idx data/parsed_idx_output.json \
     --parsed-non-idx data/parsed_non_idx_output.json \
     --ingestion data/ingestion.json
   ```
5. Workflow notifications (optional):
   ```bash
   python -m src.workflow.runner
   ```
The orchestrator (`src/pipeline/orchestrator.py`) can script these into a single run.

## Environment Knobs (common)
- Networking: `PROXY`, `HTTP_PROXY`, `HTTPS_PROXY`.
- Supabase: `SUPABASE_URL`, `SUPABASE_KEY`.
- Email: `AWS_REGION`/`AWS_DEFAULT_REGION`, `SES_FROM_EMAIL`, `ALERT_TO_EMAIL`/`ALERT_CC_EMAIL`/`ALERT_BCC_EMAIL`.
- Parser/company map: `COMPANY_MAP_FILE` (default `data/company/company_map.json`).
- Translation (optional): `GEMINI_API_KEY`, `GEMINI_PURPOSE_ENABLED` (0/1), `GOOGLETRANS_ENABLED`.
- Alert thresholds and price sanity: see `src/config/config.py` (overridable via env).

## Outputs (by stage)
- Ingestion: `data/ingestion.json` (normalized announcements).
- Downloader: PDFs in `downloads/idx-format` / `downloads/non-idx-format`; `data/downloaded_pdfs.json`; alerts in `alerts/alerts_not_inserted_downloader.json`.
- Parser: `data/parsed_idx_output.json`, `data/parsed_non_idx_output.json`; parser alerts in `alerts/`.
- Generation: uploaded filings to Supabase; alert JSON/JSONL in `alerts/`; artifact zips in `artifacts/`; optional alert emails.
- Workflow: channel messages based on user workflows + MV rows.

## Alert Policy
Full table lives in `docs/ALERT.md`. Core meanings:
- `category=not_inserted`: download/parse failed → no records.
- `category=inserted`: records uploaded but flagged for review.
- `stage`: `downloader` | `parser` | `filings`; severities `fatal/hard/warning/soft`.

## Module Map (detail)
- `src/common`: logging (`log.py`), proxy/env (`env.py`), safe file I/O (`files.py`), WIB time helpers (`datetime.py`), string/number parsers, minimal HTTP (`http.py`), Supabase REST helper (`sb.py`).
- `src/config/config.py`: thresholds for price sanity, alert gating, default paths/filenames, email defaults.
- `src/ingestion`: `runner.py` (range/window fetch + dedupe), `client.py` (IDX API with proxies), `utils/filters.py` (WIB windows), `utils/normalizer.py` (announcement schema), `cli.py` (modes: day/range/month/span).
- `src/downloader`: `runner.py` (classify, download with retries, alerts, metadata), `client.py` (requests UA + referer), `utils/classifier.py` (fuzzy matching), `cli.py`.
- `src/parser`: `base_parser.py` (pdfminer control, alert helpers, mapping PDFs → announcements); `parser_idx.py` (IDX English form, symbol resolution, transaction rows, tags); `parser_non_idx.py` (tabular attachments); `cli.py`.
- `src/core`: `types.py` (`FilingRecord`, DB-safe serialization); `transformer.py` (transform parsed dicts to records, enrich sector, translate purpose, infer tx type, build title/body/tags).
- `src/generate/filings`: `cli.py` (main entry), `utils/pipeline.py` (orchestration), `utils/provider.py` (company info), `utils/processors.py` (price/percent checks), `utils/consolidators.py` (merge IDX + Non-IDX), uploads via `services/upload`.
- `src/services`: alerts schema/context, upload (Supabase/dedup/artifacts), email (SES/sendgrid), WhatsApp (Twilio), Sheets (gspread).
- `src/workflow`: `engine.py` (load workflows + MV, build events), `runner.py` (dispatch to channels), `rules.py` (matching), `config.py`/`models.py`.
- `src/pipeline/orchestrator.py`: helper tasks (clean outputs, compute WIB windows, ensure company map, bucketize alerts, send consolidated emails, zip artifacts).
- `src/scripts`: ad-hoc helpers (`fetch_filings.py`, `company_report.py`, `company_map_hybrid.py`, etc.).

## Conventions
- Timezone: WIB (UTC+7) for ingestion windows/timestamps.
- Filings schema: see `src/core/types.py` for allowed columns and serialization rules.
- Atomic writes for alerts/JSON outputs; avoid editing generated alert files by hand.

