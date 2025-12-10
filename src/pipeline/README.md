# Pipeline Orchestrator

`orchestrator.py` is a glue script to chain ingestion → download → parse → generate/upload → alerts/artifacts/email. It also provides helpers to clean outputs, compute time windows, and manage company map assets. Use it for scheduled/batch runs when you want all stages executed in one flow.

## File: `orchestrator.py`

### Responsibilities
- Logging setup and WIB time utilities.
- Safe pre-clean of outputs (downloads/data/alerts/artifacts).
- Compute WIB windows based on minutes or explicit ranges.
- Ensure company map assets via external script.
- Run generation/upload, bucketize alerts, create artifacts zips, and send consolidated emails.
- Relocate alert files into expected locations for downstream steps.

### Critical Functions / Helpers
- Logging/time:
  - `_setup_logging(verbose)`: configure global logging.
  - `_now_wib()`, `_fmt(dt, fmt)`: WIB-aware timestamps/formatting.
- IO helpers:
  - `_safe_mkdirs(*dirs)`, `_glob_many(patterns)`: ensure/match paths.
  - `pre_clean_outputs()`: remove known outputs (downloads/alerts/artifacts/data JSON) safely and recreate dirs.
- Window computation:
  - `_compute_window_from_minutes(window_minutes) -> (date_yyyymmdd, start_hhmm, end_hhmm, stub)`: derive WIB window and stub string for outputs.
- Alerts:
  - `_relocate_alerts_to_alerts_folder(inserted_path, not_inserted_path)`: move alert files into `alerts/` so bucketizer can find them.
- Company map:
  - `_run_company_map_cli(script_path, subcmd)`: run external company map script (`get|refresh|reset|status|print`).
  - `step_company_map_ensure(script_path)`: try `get`, fallback to `refresh`.
  - `step_company_map_refresh/script_path/status`: wrappers for map maintenance.
- Artifacts/alerts/email:
  - `make_artifact_zip` (via `services.upload.artifacts`) to package alerts/inputs.
  - `bucketize_alerts` (via `services.email.bucketize`) to prepare email buckets.
  - `send_attachments` and `_render_email_content` to email consolidated artifacts.

### Typical Usage (conceptual)
While `orchestrator.py` is not wired to a single CLI, you can import and script steps:
```python
from src.pipeline.orchestrator import (
    pre_clean_outputs, _compute_window_from_minutes,
    step_company_map_ensure, run_generate, bucketize_alerts, make_artifact_zip
)

# Example: clean → compute window → run generation → bucketize alerts → zip artifacts
pre_clean_outputs()
date, sh, eh, stub = _compute_window_from_minutes(120)  # last 2 hours WIB
# ...call ingestion/downloader/parser with the window...
# ...call generate/filings runner...
ins_path, not_ins_path = _relocate_alerts_to_alerts_folder(inserted_path, not_inserted_path)
make_artifact_zip(...)
bucketize_alerts(...)
send_attachments(...)
```
Adjust paths/env to your deployment; orchestrator exposes building blocks rather than one monolithic `main`.

### Inputs & Outputs
- Inputs: downstream stage outputs (announcements JSON, downloads, parsed outputs, alerts) and optional company map script.
- Outputs: cleaned directories, relocated alerts, artifact zips, and sent emails (when invoked).

### Environment/Config Touchpoints
- Relies on other modules’ env (Supabase, email, proxies) when invoking their functions.
- No standalone config here; uses shared helpers and services modules.

### Extend/Modify
- Add CLI wrapper around orchestrator if you want a single-command end-to-end run.
- Extend pre-clean targets or add safeguards for new artifact locations.
- Customize window computation or add cron-friendly helpers.
- Integrate additional post-processing steps (e.g., pushing artifacts to S3) after `make_artifact_zip`.
