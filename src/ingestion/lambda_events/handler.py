from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from typing import Any, Dict, List

import boto3

from ingestion.http import WistiaClient
from ingestion.settings import Settings
from botocore.exceptions import ClientError


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


def _load_wistia_secret() -> dict:
    arn = os.getenv("WISTIA_SECRET_ARN")
    if not arn:
        token = os.getenv("WISTIA_API_TOKEN")
        media_ids_val = os.getenv("MEDIA_IDS", "")
        media_ids = [x.strip() for x in media_ids_val.split(",") if x.strip()]
        if not token:
            raise RuntimeError(
                "Missing WISTIA_API_TOKEN; set WISTIA_SECRET_ARN for prod "
                "or WISTIA_API_TOKEN for local/tests."
            )
        return {"api_token": token, "media_ids": media_ids}

    sm = boto3.client("secretsmanager")
    try:
        resp = sm.get_secret_value(SecretId=arn)
    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        raise RuntimeError(f"Failed to read secret {arn}: {msg}") from e

    secret = (resp.get("SecretString") or "").strip()

    # Try JSON first
    try:
        obj = json.loads(secret)
        token = obj.get("WISTIA_API_TOKEN") or obj.get("api_token")
        media_ids_val = obj.get("MEDIA_IDS") or obj.get("media_ids") or []
        if isinstance(media_ids_val, str):
            media_ids = [x.strip() for x in media_ids_val.split(",") if x.strip()]
        elif isinstance(media_ids_val, list):
            media_ids = [str(x).strip() for x in media_ids_val if str(x).strip()]
        else:
            media_ids = []
        if not token:
            raise ValueError("Secret missing 'WISTIA_API_TOKEN'/'api_token'")
        return {"api_token": token, "media_ids": media_ids}
    except json.JSONDecodeError:
        # Treat secret as raw token string
        if not secret:
            raise RuntimeError(f"Secret {arn} is empty")
        return {"api_token": secret, "media_ids": []}


def _merge_secret_into_env(secret: dict) -> None:
    # Let explicit env override secret (so setdefault, not overwrite).
    token = secret.get("api_token")
    if token:
        os.environ.setdefault("WISTIA_API_TOKEN", token)

    media_ids = secret.get("media_ids")
    if media_ids:
        if isinstance(media_ids, list):
            media_ids = ",".join(media_ids)
        os.environ.setdefault("MEDIA_IDS", media_ids)

    ip_hmac = secret.get("hmac_secret") or secret.get("ip_hash_key")
    if ip_hmac:
        os.environ.setdefault("VISITOR_IP_HMAC_KEY", ip_hmac)


def handler(event, context):
    """
    Event (Step Functions or manual):
    {
      "day": "YYYY-MM-DD",          # optional; defaults to yesterday
      "media_ids": ["abc","def"]    # optional; defaults to Settings.media_ids
    }
    """
    wistia_secrets = _load_wistia_secret()
    _merge_secret_into_env(wistia_secrets)

    cfg = Settings.from_env()
    event_media_ids = event.get("media_ids") if isinstance(event, dict) else None
    media_ids = event_media_ids or (cfg.media_ids or [])

    client = WistiaClient(
        base_url=cfg.base_url,
        token=wistia_secrets.get("api_token"),
        timeout_s=cfg.request_timeout_s,
    )

    s3 = boto3.client("s3")
    target_day = _parse_date(
        event.get("day") if isinstance(event, dict) else None, _today_utc()
    )
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
