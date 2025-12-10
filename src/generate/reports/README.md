# Generate Reports Module

Builds reporting artifacts (e.g., email/SMS/WA summaries, JSON reports) from filings/alerts and can send them through configured channels.

## Directory Contents
- `cli.py` — entrypoint to generate/send reports.
- `wa_cli.py` — WhatsApp-specific report sender.
- `core.py` — shared report-building logic (formatting, aggregation).
- `mailer.py` — email report helpers (render/send).
- `__init__.py` — package marker.

## Critical Functions / Responsibilities
- `cli.py`
  - `main()`: parse args (alerts/filings inputs, output path, channel options), load env, call report build and optional send.
- `wa_cli.py`
  - `main()`: WhatsApp-focused CLI; parse args and send WA-formatted summaries.
- `core.py`
  - Aggregation/formatting helpers: counts by severity/code, top codes, short URLs, concise summaries per channel.
- `mailer.py`
  - Email composition/sending: build subject/body, attach JSON reports/alerts, send via SES/SendGrid.
- Channel senders (referenced in orchestrator/workflows)
  - WhatsApp/Twilio sender, email sender, etc., consume `core.py` outputs to deliver messages.

## Data Flow
1) **Inputs**: filings and/or alerts (typically from `generate/filings` outputs), possibly market/context data.
2) **Compose**: aggregate key stats, build human-friendly summaries for the chosen channel (email/WA/others).
3) **Send/Write**: use channel-specific helpers (`mailer.py` for email, WA sender in `wa_cli.py`) to deliver or write artifacts.

## Usage (typical)
```bash
# Email or generic report
python -m src.generate.reports.cli --alerts alerts/alerts_inserted_filings.json --out report.json

# WhatsApp-focused run
python -m src.generate.reports.wa_cli --alerts alerts/alerts_inserted_filings.json
```
Flags vary by CLI; common ones include:
- `--alerts` / `--filings` input paths.
- `--out` for JSON/report output.
- Channel credentials (e.g., sender email, Twilio/WA settings).

## Environment/Config
- Email: `SES_FROM_EMAIL`, `ALERT_TO_EMAIL`, AWS region (or SendGrid keys if used).
- WhatsApp/Twilio: account SID/token, from/to numbers.
- Other channel configs as required by your senders.

## Extending
- Add new report formats: extend `core.py` to compute/format additional metrics.
- Add channels: create a new CLI/sender and hook into `core.py` outputs.
- Customize templates: adjust `mailer.py` or channel-specific formatting.
