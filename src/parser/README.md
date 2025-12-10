# Parser Module

Parse downloaded PDFs (IDX and Non-IDX variants) into structured dictionaries ready for transformation into `FilingRecord`s. Emits parser-stage alerts for missing text, symbol issues, and structural problems.

## Directory Contents
- `cli.py` — entrypoint to run IDX / Non-IDX / both parsers.
- `base_parser.py` — shared plumbing: pdfminer noise control, alert helpers, announcement→PDF mapping, text extraction, and result saving.
- `parser_idx.py` — IDX (English) form parser: symbol resolution, holder normalization, transaction row extraction, tagging.
- `parser_non_idx.py` — Non-IDX/tabular parser (handles attachments with transaction tables).
- `utils/` — helpers:
  - `text_extractor.py`, `number_parser.py`, `name_cleaner.py`, `transaction_classifier.py`
  - `company_resolver.py` (symbol/name resolution, reverse maps)

## Data Flow
1) **Inputs**: PDFs in `downloads/idx-format` or `downloads/non-idx-format`, plus announcements JSON (default `data/ingestion.json`) to map filenames back to metadata.
2) **Extract text**: `BaseParser.extract_text_from_pdf` (pdfplumber) with pdfminer noise suppressed.
3) **Parse**:
   - IDX: `parser_idx.py` slices to English section, extracts header fields, resolves symbol, parses holder and transactions, computes tags (bullish/bearish/transfer/MESOP), builds transaction rows with ISO dates and weighted price.
   - Non-IDX: parses expected transaction table; emits `table_not_found` when missing.
4) **Validate**: `validate_parsed_data` per parser (skip all-zero/invalid holders, require transactions unless marked skip).
5) **Alerts**: parser-stage alerts written to `alerts/alerts_inserted_parser*.json` and `alerts/alerts_not_inserted_parser*.json`.
6) **Outputs**: parsed JSON files (defaults `data/parsed_idx_output.json`, `data/parsed_non_idx_output.json`).

## Alert Codes (parser stage)
- Fatal (`category=not_inserted`): `no_text_extracted`, `parse_exception`, `symbol_missing` (IDX), `table_not_found` (Non-IDX).
- Warning (`category=inserted`): `symbol_name_mismatch`, `company_resolve_ambiguous`, `validation_failed` (parsed but suspect).

## CLI Usage (`python -m src.parser.cli`)
```bash
# Run IDX parser only (defaults)
python -m src.parser.cli --parser idx \
  --idx-folder downloads/idx-format \
  --idx-output data/parsed_idx_output.json \
  --announcements data/ingestion.json

# Run both IDX and Non-IDX
python -m src.parser.cli --parser both \
  --idx-folder downloads/idx-format \
  --non-idx-folder downloads/non-idx-format \
  --announcements data/ingestion.json
```
Flags:
- `--parser idx|non-idx|both`
- `--idx-folder`, `--non-idx-folder` — PDF sources
- `--idx-output`, `--non-idx-output` — parsed JSON outputs
- `--announcements` — normalized announcements to map filenames → metadata
- `--log-level` — INFO/DEBUG, etc.

## Key Internals
- `BaseParser`
  - Logging init with pdfminer suppression (`init_logging`), `silence_pdfminer`.
  - `_build_parser_alert`, `_parser_warn`, `_parser_fail`, `_flush_parser_alerts` — consistent alert writing with context.
  - `build_pdf_mapping` — map filenames to announcement metadata (main_link + attachments).
  - `extract_text_from_pdf` — pdfplumber with per-page safeguard; saves debug text via `save_debug_output`.
  - `parse_folder` — iterate PDFs, track current alert context, call subclass `parse_single_pdf` + `validate_parsed_data`, write outputs and alerts.
- `parser_idx.py`
  - Symbol resolution: uses company map (`COMPANY_MAP_FILE` env) and `company_resolver` (reverse maps, fuzzy via rapidfuzz). Emits `symbol_missing` or `symbol_name_mismatch`.
  - Holder normalization: `NameCleaner` to classify holder type (institution vs insider) and clean names.
  - Transactions: regex over text and line-based parsing; builds `price_transaction` list with ISO `date_iso`, type, price, amount; computes doc-level `transaction_type`, totals, WAP price, transfer flags.
  - Tags: `TransactionClassifier` adds MESOP/transfer/bullish/bearish based on rows and share deltas.
  - Validation: reject when holder invalid or all-zero metrics; require transactions unless explicitly skipped.
- `parser_non_idx.py`
  - Parses tabular attachments; flags `table_not_found` when missing or too small; resolves company symbol (ambiguity → `company_resolve_ambiguous` warning).
- `utils`
  - `text_extractor`: find values in tables/lines, percentage/number search helpers.
  - `number_parser`: tolerant parsing for numbers/percentages with mixed separators.
  - `name_cleaner`: heuristics for holder type and normalization.
  - `company_resolver`: load symbol→name map, build reverse map, fuzzy resolve names, pretty-print company names.
  - `transaction_classifier`: derive flags/tags from parsed text/rows.

## Inputs & Outputs (contract)
- Inputs:
  - PDFs under `downloads/idx-format` / `downloads/non-idx-format`.
  - Announcements JSON (default `data/ingestion.json`) to provide title/url context and company code hints.
  - Company map JSON (`COMPANY_MAP_FILE`, default `data/company/company_map.json`).
- Outputs:
  - Parsed JSON (`data/parsed_idx_output.json`, `data/parsed_non_idx_output.json`).
  - Alerts: `alerts/alerts_inserted_parser.json`, `alerts/alerts_not_inserted_parser.json` (date-stamped filenames).
  - Debug text dumps in `debug_output/*.txt` when extraction succeeds.

## Environment & Config
- `COMPANY_MAP_FILE` — path to company mapping; used for symbol/name resolution.
- `COMPANY_RESOLVE_MIN_SCORE`, `COMPANY_SUGGEST_TOPK` — fuzzy thresholds for IDX parser.
- `PDF_DEBUG` (1/true) — to keep pdfminer verbose; default off (noise suppressed).
- Proxies: inherited from env for pdfplumber/httpx if needed.

## Edge Cases & Validation
- Empty/garbled PDFs → `no_text_extracted` fatal.
- Symbol not resolvable → `symbol_missing` fatal (IDX).
- Company name vs symbol mismatch → `symbol_name_mismatch` warning.
- Non-IDX missing transaction table → `table_not_found` fatal.
- Invalid holder name or all-zero metrics → skip (not inserted).
- Transaction rows optional only when explicitly marked skip (`skip_filing`).

## How to Extend
- Add new alert codes: implement in `BaseParser` helpers and call from subclass.
- Support new PDF variants: create a new subclass implementing `parse_single_pdf` and `validate_parsed_data`, then wire it in `cli.py`.
- Adjust symbol resolution: tweak thresholds/env in `company_resolver` calls or enrich `company_map.json`.
- Capture more fields: extend field extraction in parser subclasses and ensure downstream transformer handles them.

