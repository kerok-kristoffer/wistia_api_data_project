from __future__ import annotations
from datetime import date, datetime, timezone


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def parse_iso_date(value: str | None, default: date) -> date:
    if not value:
        return default
    return datetime.strptime(value, "%Y-%m-%d").date()
