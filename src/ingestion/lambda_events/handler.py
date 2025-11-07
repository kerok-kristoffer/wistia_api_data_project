from __future__ import annotations
import boto3
from typing import Any, Dict, List
from ingestion.http import WistiaClient
from ingestion.settings import Settings
from ingestion.common.dates import _today_utc, parse_iso_date
from ingestion.common.secrets import load_wistia_secret, merge_wistia_secret_into_env
from ingestion.common.s3io import put_jsonl_lines
from ingestion.common.paths import events_key


def handler(event, context):
    secret = load_wistia_secret()
    merge_wistia_secret_into_env(secret)

    # Short-circuit for CI smoke tests
    if event.get("smoke_test"):
        print("[media-ingest] Smoke test mode: skipping ingestion.")
        return {"ok": True, "smoke_test": True}

    cfg = Settings.from_env()
    target_day = parse_iso_date((event or {}).get("day"), _today_utc())
    media_ids = (event or {}).get("media_ids") or (cfg.media_ids or [])

    client = WistiaClient(
        base_url=cfg.base_url,
        token=secret["api_token"],
        timeout_s=cfg.request_timeout_s,
    )

    s3 = boto3.client("s3")
    summary = {"day": target_day.isoformat(), "media": []}

    for mid in media_ids:
        # your existing fetch_all_events() works; inline or keep it here
        rows = []
        page = 1
        while True:
            batch = client.events(
                media_id=mid,
                start_date=target_day.isoformat(),
                end_date=target_day.isoformat(),
                per_page=cfg.page_size,
                page=page,
            )
            if not batch:
                break
            rows.extend(batch)
            put_jsonl_lines(
                s3,
                bucket=cfg.s3_bucket_raw,
                key=events_key(cfg.s3_prefix_raw, target_day, mid, page),
                rows=batch,
            )
            if len(batch) < cfg.page_size:
                break
            page += 1

        summary["media"].append({"media_id": mid, "rows": len(rows)})

    return summary


def fetch_all_events(client, media_id: str, day, per_page: int) -> List[Dict[str, Any]]:
    """
    Pull all pages for a media_id on a single day window.
    Kept for backward compatibility with tests.
    """
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
