# Core Module

Defines the canonical filing data model and the transformer that converts parsed dictionaries into upload-ready `FilingRecord`s with enriched metadata (sectors, tags, titles, purpose translation, price aggregation).

## Directory Contents
- `types.py` — `FilingRecord`, `PriceTransaction`, allowed DB columns, percentage helpers.
- `transformer.py` — raw→record transformer: resolve source/date, normalize symbol/sector/sub-sector, translate purpose, infer transaction type, aggregate transactions, build title/body/tags, and set audit flags.

## Key Concepts
- **FilingRecord**: single canonical row to insert into Supabase/DB, with holdings, percentages, price/value, tags, sector/sub-sector, source, and audit flags.
- **PriceTransaction**: per-transaction detail (date, type, price, amount) used to build `price_transaction` JSONB for DB.
- **Allowed columns**: controlled set (`FILINGS_ALLOWED_COLUMNS` in `types.py` and `ALLOWED_DB_COLUMNS` in `FilingRecord.to_db_dict`) to prevent accidental schema drift.

## Critical Functions / Responsibilities
- `types.py`
  - `FilingRecord.to_db_dict()`: serialize dataclass to DB-safe dict; collapse `price_transaction` to array-of-dicts, normalize percentages (floor to 5 decimals), ensure sector/sub_sector defaults to `"unknown"`, enforce allowed columns only.
  - `PriceTransaction`: lightweight container used by transformer and DB serialization.
  - Helpers: `floor_pct_5`, `close_pct`, `FILINGS_ALLOWED_COLUMNS` (public allowlist).
- `transformer.py`
  - `transform_raw_to_record(raw_dict, ingestion_map, company_map=None) -> FilingRecord`:
    - Purpose translation (Google Translate or Gemini optional) to English, then tag mapping.
    - Holdings parsing and transaction type inference (buy/sell/share-transfer/other).
    - Percentage rounding to 5 decimals.
    - Source/date resolution: reconcile parser source hints with ingestion map (URL + date); fallbacks to raw fields.
    - Build `price_transaction` list from either list-of-dicts or legacy dict-of-lists; compute weighted average price/value, amount/transaction value fallbacks.
    - Title/body generation: human-friendly text summarizing holder/company/action/purpose.
    - Tags: normalize provided tags, add purpose-derived tags, add bullish/bearish from tx type or holdings/% delta.
    - Sector/sub-sector enrichment: kebab-case and fill unknowns via provider (company map cache).
    - Audit flags: mark share-transfer/other for manual UID pairing.
  - `transform_many(raw_dicts, ingestion_map, company_map=None) -> List[FilingRecord]`: vector wrapper with error guard per row.

## Data Flow (per filing)
1) Parsed dict from parser (IDX/Non-IDX) + ingestion map entry.
2) Translate purpose → English (if enabled) and map to purpose tags.
3) Normalize holdings/percentages and infer transaction type (fall back to `other`).
4) Resolve source URL/date from ingestion map; fallback to raw fields.
5) Build `price_transaction` list; compute WAP price/value/amount if missing.
6) Generate title/body and tags (purpose + bullish/bearish).
7) Enrich sector/sub-sector from provider/company map; kebab-case outputs.
8) Emit `FilingRecord` with audit flags (e.g., transfer needs manual UID).
9) `to_db_dict` ensures safe serialization for upload.

## Inputs & Outputs (contract)
- Inputs: parsed filing dicts (from parser), ingestion map (`filename → date/url/meta`), optional company map/provider.
- Output: `FilingRecord` objects; when serialized via `to_db_dict`, only allowed DB columns and normalized fields are present.

## Environment/Config Touchpoints
- Purpose translation: `GEMINI_PURPOSE_ENABLED`, `GEMINI_API_KEY`, `GEMINI_MODEL`, `GEMINI_PURPOSE_TIMEOUT`, `GEMINI_ALLOW_PROXY`, `GOOGLETRANS_ENABLED`, `GOOGLETRANS_ALLOW_PROXY`.
- Sector enrichment: `FILINGS_COMPANY_MAP`/provider data (`src/generate/filings/utils/provider`).
- Proxy awareness: translation may skip when proxies are present unless explicitly allowed.

## Edge Cases & Behavior
- If price/value missing but price_transactions exist, WAP and totals fill gaps.
- Percentages floored to 5 decimals to match DB tolerance; `close_pct` uses absolute pp tolerance.
- If sector/sub-sector missing or list, defaults to `"unknown"` string.
- Symbol normalization appends `.JK` when absent; symbol resolution is lenient but will leave `None` if missing.
- If parser emits `skip_filing`, upstream should respect and avoid transformation (handled before transformer call).

## Extend/Modify
- Add new derived fields: extend `FilingRecord`, update `to_db_dict` allowlist, and ensure downstream upload columns match.
- Customize title/body/tags: tweak `_generate_title_and_body`, `_normalize_tags`, `_apply_bull_bear_tags`.
- Modify purpose translation behavior: adjust translation clients or mappings in `transformer.py`.
- Change sector enrichment logic: update `_enrich_sector_from_provider` or `provider` implementation.
