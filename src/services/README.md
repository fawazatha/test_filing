# Services Module

Shared service layer used by generation/workflow/orchestrator: alert schema/context, upload to Supabase, artifacts handling, email/WhatsApp delivery, and Sheets helpers.

## Alerts (`services/alert`)
- `schema.py`: `build_alert(...)` to standardize alert payloads (category, stage, code, severity, context, reasons).
- `ingestion_context.py`: link alerts back to announcements (build ingestion index, resolve context from announcement).
- Used by downloader/parser/generation to emit consistent alert JSON.

## Upload (`services/upload`)
- `supabase.py`: `SupabaseUploader` for DB inserts; handles connection to Supabase REST/DB.
- `dedup.py`: `upload_filings_with_dedup(...)` to skip existing filings before insert.
- `paths.py`: utilities to resolve data/alerts paths for uploads.
- `artifacts.py`: helpers to build artifact zip bundles (alerts + context) for audit/sharing.

## Email (`services/email`)
- `ses_email.py`: send attachments via AWS SES.
- `mailer.py`: render email content/templates for alerts.
- `bucketize.py`: group alerts into buckets (inserted vs not_inserted) for emailing.
- `manager.py`: higher-level orchestration for email sending.
- `notifier.py`: notifier interface/wrapper.

## WhatsApp (`services/whatsapp`)
- `twilio_sender.py`: send messages via Twilio API.
- `whatsapp_formatter.py`: format filings/alerts for WhatsApp delivery.

## Sheets (`services/sheet`)
- `google.py`: helpers for gspread/Google Sheets updates.

## Responsibilities & Data Flow
- Alerts: downstream stages call `build_alert` to produce consistent JSON, optionally enriched with ingestion context; `bucketize` groups them for email.
- Upload: filings (after generation) go through dedup + Supabase uploader; artifacts zip can be produced from alert files.
- Email/WhatsApp: format and send alert summaries or messages to operators; often triggered by orchestrator/generation.
- Sheets: update Google Sheets with workflow outputs when configured.

## Environment / Config (common)
- Supabase: `SUPABASE_URL`, `SUPABASE_KEY`.
- Email/SES: `AWS_REGION`/`AWS_DEFAULT_REGION`, `SES_FROM_EMAIL`, recipients (`ALERT_TO_EMAIL`, `ALERT_CC_EMAIL`, `ALERT_BCC_EMAIL`).
- WhatsApp/Twilio: account SID/token, from/to numbers as required by `twilio_sender`.
- Sheets: Google credentials/config as needed by `gspread`.

## Extend/Modify
- Add new alert fields: extend `build_alert` and ensure consumers handle them.
- Change dedup logic: adjust `upload/dedup.py`.
- Add channels: create new sender/formatter modules and wire them in orchestrator/workflow.
- Customize email formatting: tweak `mailer.py` or bucketization strategy.
