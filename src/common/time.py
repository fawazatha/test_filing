from datetime import datetime, timezone, timedelta

"""Time-related helpers for consistent timestamps."""

# Asia/Jakarta (UTC+7) without external tz dependencies
JAKARTA_TZ = timezone(timedelta(hours=7))


def timestamp_jakarta() -> str:
    """Return ISO timestamp in Asia/Jakarta timezone (YYYY-MM-DDTHH:MM:SS)."""
    return datetime.now(JAKARTA_TZ).replace(microsecond=0).isoformat()
