from __future__ import annotations
import json
from datetime import date, datetime, timezone
from typing import List
import boto3

from ingestion.common.secrets import load_wistia_secret  # <- same helper used elsewhere
from ingestion.http import WistiaClient
from ingestion.settings import (
    Settings,
)  # S3_BUCKET_RAW, S3_PREFIX_RAW, BASE_URL, TIMEOUT_S


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def _parse_date(val: str | None, default: date) -> date:
    if not val:
        return default
    return datetime.strptime(val, "%Y-%m-%d").date()


def _media_key(prefix: str, day: date, media_id: str) -> str:
    # raw/wistia/media/dt=YYYY-MM-DD/media_id=<id>/object.json
    return f"{prefix.rstrip('/')}/dt={day.isoformat()}/media_id={media_id}/object.json"


def handler(event, context):
    """
    Event (Step Functions/manual):
    {
      "day": "YYYY-MM-DD",      # optional; defaults to today UTC
      "media_ids": ["abc"]      # optional; defaults to Secret MEDIA_IDS (if present)
    }
    Writes one JSON object per media to:
      s3://<S3_BUCKET_RAW>/<S3_PREFIX_RAW>/media/dt=YYYY-MM-DD/media_id=<id>/object.json
    """
    # Load from the SAME helper your other Lambdas use (expects WISTIA_SECRET_ARN in env)
    secret = load_wistia_secret()  # -> {"api_token": "...", "media_ids": [...]?}

    token = secret.get("api_token")
    if not token:
        # Keep error consistent with your other handlers
        raise RuntimeError("Missing WISTIA_API_TOKEN (via WISTIA_SECRET_ARN or env).")

    cfg = Settings.from_env()
    s3 = boto3.client("s3")

    # (Optional but handy when debugging roles)
    sts = boto3.client("sts")
    print("[ROLE] CallerIdentity:", json.dumps(sts.get_caller_identity()))

    target_day = _parse_date(
        event.get("day") if isinstance(event, dict) else None, _today_utc()
    )
    event_ids = event.get("media_ids") if isinstance(event, dict) else None
    media_ids: List[str] = event_ids or secret.get("media_ids") or []
    if not media_ids:
        raise RuntimeError(
            "No media_ids provided (event.media_ids empty and Secret has none)."
        )

    client = WistiaClient(
        base_url=cfg.base_url,  # e.g. https://api.wistia.com/v1
        token=token,  # from secret via ARN
        timeout_s=cfg.request_timeout_s,
    )

    summary = {"day": target_day.isoformat(), "media": []}
    for mid in media_ids:
        data = client.medias_get(media_id=mid)  # you already added this to WistiaClient
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        key = _media_key(cfg.s3_prefix_raw, target_day, mid)

        print(f"[MEDIA] Writing {len(body)} bytes to s3://{cfg.s3_bucket_raw}/{key}")
        s3.put_object(Bucket=cfg.s3_bucket_raw, Key=key, Body=body)

        summary["media"].append({"media_id": mid, "bytes": len(body)})

    return summary
