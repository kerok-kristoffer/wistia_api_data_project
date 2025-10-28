from __future__ import annotations
from datetime import date


def events_key(prefix: str, day: date, media_id: str, page: int) -> str:
    return f"{prefix.rstrip('/')}/events/dt={day.isoformat()}/media_id={media_id}/page={page:04d}.jsonl"


def visitors_key(prefix: str, day: date, media_id: str, page: int) -> str:
    return f"{prefix.rstrip('/')}/visitors/dt={day.isoformat()}/media_id={media_id}/page={page:04d}.jsonl"
