# Generate Filings Module

Main post-parse pipeline: ingest parsed IDX/Non-IDX JSON, convert to canonical `FilingRecord`s, run validations/alerts, deduplicate, upload to Supabase, and emit artifacts/emails. This is the handoff layer between parsing and downstream distribution/workflows.

## Directory Contents
- `cli.py` — user-facing entrypoint; wires inputs, upload/email options, and logs.
- `runner.py` — thin wrapper calling `utils/pipeline.py`.
- `utils/pipeline.py` — orchestrates the end-to-end run (load → transform → process → consolidate → upload → artifacts/email).
- `utils/loaders.py` — load parsed IDX/Non-IDX files, ingestion map, optional market/company data.
- `utils/processors.py` — price sanity checks, percent discrepancy checks, mismatch detection, alert assembly.
- `utils/consolidators.py` — merge IDX + Non-IDX streams, resolve duplicates.
- `utils/provider.py` — company info lookup (sector/sub-sector) with local cache/fallback.
- `utils/types.py` — helper dataclasses/types used inside the generation pipeline.

## Critical Functions / Responsibilities
- `cli.py`
  - `main()`: parse CLI, load env, resolve recipients, invoke `run_generate` (pipeline).
- `utils/pipeline.py`
  - `run(...)`: orchestrates the full pipeline: load inputs → transform → process alerts → consolidate → upload → artifacts/email.
  - `load_inputs(...)`: call loaders to read parsed IDX/Non-IDX + ingestion map; prep auxiliary data.
  - `build_records(...)`: wraps `transform_many` to produce `FilingRecord`s and propagate audit flags.
  - `write_alerts(...)`: write inserted/not_inserted alerts to `alerts/` (JSON/JSONL).
  - `make_artifacts(...)`: bundle alerts/context into `artifacts/*.zip`.
- `utils/loaders.py`
  - `load_parsed_idx/non_idx(...)`: read parsed JSON (tolerant to missing files).
  - `load_ingestion_map(...)`: map filenames → announcement metadata (date/source).
  - `load_company_info/provider cache`: helper for sector/sub-sector enrichment.
- `utils/processors.py`
  - `apply_price_checks(...)`: within-doc median vs tx price, market sanity, zero-missing heuristics.
  - `apply_percent_checks(...)`: recompute ownership % vs reported; flag discrepancies.
  - `apply_logic_checks(...)`: transaction type mismatch, transfer UID requirement.
  - Each emits alert dicts with `category/stage/code/severity/ctx`.
- `utils/consolidators.py`
  - `consolidate(...)`: merge IDX + Non-IDX streams, resolve duplicates, choose best records.
- `services.upload.*`
  - `dedup.upload_filings_with_dedup(...)`: skip existing rows before insert.
  - `supabase.SupabaseUploader`: low-level insert client.
- `src.core.transformer.transform_many`
  - Converts parsed dicts → `FilingRecord` (title/body/tags/purpose translation/sector enrichment, normalized `price_transaction`).

## End-to-End Flow (per run)
1) **Load inputs**  
   - Parsed filings: `data/parsed_idx_output.json`, `data/parsed_non_idx_output.json`.  
   - Ingestion map: `data/ingestion.json` (for date/source resolution).  
   - Company map/market data as configured.
2) **Transform to canonical** (`src.core.transformer.transform_many`)  
   - Resolve symbols, sectors, sub-sectors (via `provider`).  
   - Normalize timestamps/source URLs from ingestion map.  
   - Translate purpose (GoogleTranslate/Gemini optional).  
   - Build title/body/tags and price_transaction list.  
   - Output: list of `FilingRecord`.
3) **Process/validate** (`utils/processors`)  
   - Price sanity: within-document median vs transaction price.  
   - Market sanity: compare vs market reference (Close/VWAP) with lookback.  
   - Zero-missing heuristics (x10/x100 anomalies).  
   - Ownership % recompute vs reported (`percent_discrepancy`, `delta_pp_mismatch`).  
   - Transaction type mismatches (buy/sell vs holdings delta).  
   - Missing/stale price flags.  
   - Transfer UID/manual review flags.  
   - Emit alerts with severity/category (`inserted` vs `not_inserted`).
4) **Consolidate** (`utils/consolidators`)  
   - Merge IDX + Non-IDX records, handle duplicates, pick best versions.
5) **Upload**  
   - Deduplicate vs DB (`services.upload.dedup`) and insert via `services.upload.supabase`.  
   - Allowed columns derived from `FilingRecord.to_db_dict()`/`ALLOWED_DB_COLUMNS` in `cli.py`.
6) **Artifacts & email (optional)**  
   - Write alert JSON/JSONL into `alerts/`.  
   - Zip artifacts (alerts + context) into `artifacts/`.  
   - Send alert emails via `services.email` (SES/SendGrid) with bucketized attachments.

## CLI Usage (`python -m src.generate.filings.cli`)
```bash
python -m src.generate.filings.cli \
  --parsed-idx data/parsed_idx_output.json \
  --parsed-non-idx data/parsed_non_idx_output.json \
  --ingestion data/ingestion.json \
  [--upload 1] [--dedup 1] [--dry-run] \
  [--to a@x.com,b@y.com] [--cc c@z.com] [--bcc d@w.com] \
  [--from-email no-reply@example.com] [--aws-region ap-southeast-1]
```
Notes:
- `--upload` / `--dedup`: control Supabase insertion behavior; `--dry-run` skips writes.
- Recipients/from/region can also come from env (`ALERT_TO_EMAIL`, `ALERT_CC_EMAIL`, `ALERT_BCC_EMAIL`, `SES_FROM_EMAIL`, `AWS_REGION`).
- Logging level via `LOG_LEVEL` env (see `src/config/config.py`).

## Alert Codes (filings stage)
- Price: `price_deviation_within_doc`, `price_deviation_vs_market`, `possible_zero_missing`.
- Ownership %: `percent_discrepancy`, `delta_pp_mismatch`.
- Price data: `missing_price`, `stale_price`.
- Logic: `mismatch_transaction_type`, `transfer_uid_required`.
- Category/severity align with global policy (`docs/ALERT.md`); alerts are bucketized into `alerts/alerts_inserted_filings*.json` and `alerts/alerts_not_inserted_filings*.json`.

## Inputs & Outputs (contract)
- Inputs: parsed IDX/Non-IDX JSON; ingestion map JSON; company map/market data files as configured.
- Outputs:
  - `alerts/` JSON/JSONL (inserted vs not_inserted).
  - Artifact zip in `artifacts/` (alerts + sometimes inputs for auditability).
  - Supabase rows inserted (when upload enabled).
  - Console logs summarizing counts (inserted/skipped/alerted).

## Configuration & Env (key knobs)
- Thresholds: `src/config/config.py` (env overrides) — price ratios, zero-missing bounds, percent tolerance, market lookback, gating reasons for email.
- Supabase: `SUPABASE_URL`, `SUPABASE_KEY`.
- Email/SES: `AWS_REGION`/`AWS_DEFAULT_REGION`, `SES_FROM_EMAIL`, `ALERT_TO_EMAIL`, `ALERT_CC_EMAIL`, `ALERT_BCC_EMAIL`.
- Purpose translation: `GEMINI_API_KEY`, `GEMINI_PURPOSE_ENABLED`, `GOOGLETRANS_ENABLED`.
- Company/price data paths: `FILINGS_COMPANY_MAP`, `FILINGS_LATEST_PRICES`.

## Edge Cases & Behavior
- Missing market data → `missing_price`/`stale_price` alerts; rows may still insert.
- Strong price outliers trigger warnings/hard alerts; policy can gate emails (`GATE_REASONS` in config).
- Transfer/other transaction types add `transfer_uid_required` for manual pairing.
- Percent tolerance is in percentage points (not %); defaults in config.
- `to_db_dict()` ensures sector/sub_sector default to `"unknown"` to satisfy NOT NULL constraints.

## Extend/Modify
- Add validations/alerts: implement in `utils/processors.py`, register alert code/severity, ensure alert writing.
- Change dedupe/upload: adjust `services.upload.dedup` or `services.upload.supabase` usage in `utils/pipeline.py`.
- Enrich records: extend `src.core.transformer` and `FilingRecord` to carry new fields; update `ALLOWED_DB_COLUMNS` in `cli.py` if uploading them.
- Additional outputs: modify `utils/pipeline.py` to emit extra reports before upload/artifact creation.
