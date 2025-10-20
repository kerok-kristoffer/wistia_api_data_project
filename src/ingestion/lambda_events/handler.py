from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any, Dict, List

import boto3

from ingestion.http import WistiaClient
from ingestion.settings import Settings


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def _parse_date(val: str | None, default: date) -> date:
    if not val:
        return default
    return datetime.strptime(val, "%Y-%m-%d").date()


def _jsonl_bytes(rows: List[Dict[str, Any]]) -> bytes:
    return ("\n".join(json.dumps(r, ensure_ascii=False) for r in rows)).encode("utf-8")


def _events_key(prefix: str, day: date, media_id: str, page: int) -> str:
    # {prefix}/events/dt=YYYY-MM-DD/media_id=.../page=NNNN.jsonl
    return f"{prefix.rstrip('/')}/events/dt={day.isoformat()}/media_id={media_id}/page={page:04d}.jsonl"


def fetch_all_events(
    client: WistiaClient, media_id: str, day: date, per_page: int
) -> List[Dict[str, Any]]:
    """Pull all pages for a media_id on a single day window."""
    rows: List[Dict[str, Any]] = []
    page = 1
    while True:
        batch = client.events(
            media_id=media_id,
            start_date=day.isoformat(),
            end_date=day.isoformat(),
            per_page=per_page,
            page=page,
        )
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < per_page:
            break
        page += 1
    return rows


def handler(event, context):
    """
    Event (Step Functions or manual):
    {
      "day": "YYYY-MM-DD",          # optional; defaults to yesterday
      "media_ids": ["abc","def"]    # optional; defaults to Settings.media_ids
    }
    """
    cfg = Settings.from_env()

    target_day = _parse_date(
        event.get("day") if isinstance(event, dict) else None, _today_utc()
    )
    media_ids = (
        event.get("media_ids") if isinstance(event, dict) else None
    ) or cfg.media_ids

    client = WistiaClient(
        base_url=cfg.base_url,
        token=cfg.api_token,
        timeout_s=cfg.request_timeout_s,
    )

    s3 = boto3.client("s3")
    summary = {"day": target_day.isoformat(), "media": []}

    for mid in media_ids:
        all_rows = fetch_all_events(client, mid, target_day, cfg.page_size)

        # write in chunks of page_size (keeps file sizes manageable)
        page = 1
        for i in range(0, len(all_rows), cfg.page_size):
            chunk = all_rows[i : i + cfg.page_size]
            key = _events_key(cfg.s3_prefix_raw, target_day, mid, page)
            s3.put_object(Bucket=cfg.s3_bucket_raw, Key=key, Body=_jsonl_bytes(chunk))
            page += 1

        summary["media"].append({"media_id": mid, "rows": len(all_rows)})

    return summary
