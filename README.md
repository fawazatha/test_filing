# sectors_idx_filing_pipeline



# Alerts Policy

## 1. Core meanings

- **`category = not_inserted`**  
  → The document produced **no usable data**  
  (download failed / unsupported / parsing failed).

- **`category = inserted`**  
  → A filing **was inserted**, but there is something **potentially wrong or ambiguous** that should be reviewed.

- **`stage`**:
  - `downloader` – getting the file.
  - `parser` – turning PDF into structured fields.
  - `filings` – normalizing + checking numbers.

Severity legend:

- **fatal** → no records should be produced from this document.  
- **hard** → serious anomaly on an inserted record; likely needs manual correction.  
- **warning / soft** → anomaly or low-confidence case; should be visible but not block insertion.


## 2. Alert Reference Table

| Stage           | Code                        | Category      | Severity      | Description (short, direct) |
|-----------------|-----------------------------|--------------|---------------|-----------------------------|
| downloader      | `low_title_similarity`      | not_inserted | fatal         | Announcement title and expected filename are too different → skip download. |
| downloader      | `download_failed`           | not_inserted | fatal         | All download attempts failed → no PDF available. |
| downloader      | `unsupported_format`        | not_inserted | fatal         | File is not a valid / readable PDF (wrong format, corrupt, or wrong content). |
| parser (IDX)    | `no_text_extracted`         | not_inserted | fatal         | PDF text extraction returned nothing usable → cannot parse. |
| parser (IDX/Non-IDX) | `parse_exception`      | not_inserted | fatal         | Unhandled error during parsing → document cannot be processed. |
| parser (IDX)    | `symbol_name_mismatch`      | inserted     | warning       | Symbol resolved, but company name in PDF does not match canonical name. |
| parser (IDX, optional) | `symbol_missing`     | not_inserted | fatal         | No issuer symbol can be determined at all → cannot link filing to an issuer. |
| parser (Non-IDX)| `table_not_found`           | not_inserted | fatal         | Expected transaction table on the last page is missing or too small → no transactions. |
| parser (Non-IDX)| `company_resolve_ambiguous` | inserted     | warning       | Symbol chosen from low-confidence / ambiguous issuer matches. |
| filings         | `price_deviation_within_doc`| inserted     | soft          | Transaction price is a strong outlier vs median price **within the same document**. |
| filings         | `price_deviation_vs_market` | inserted     | warning       | Transaction price is far from recent market reference (Close / VWAP). |
| filings         | `possible_zero_missing`     | inserted     | warning / hard| Price looks ×10/×100 off vs reference → likely missing zero / wrong scale. |
| filings         | `percent_discrepancy`       | inserted     | hard          | Calculated ownership % vs reported % differ beyond tolerance. |
| filings         | `missing_price`             | inserted     | soft          | No market price data available for the relevant date range. |
| filings         | `stale_price`               | inserted     | soft          | Market price data is older than the configured lookback window. |
| filings         | `mismatch_transaction_type` | inserted     | hard          | `buy`/`sell` label disagrees with change in holdings / ownership %. |
| filings         | `transfer_uid_required`     | inserted     | warning       | `share-transfer` / `other` transaction that needs manual UID pairing to match both sides. |
