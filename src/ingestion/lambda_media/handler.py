from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError

from ingestion.http import WistiaClient
from ingestion.settings import (
    Settings,
)  # expects S3_BUCKET_RAW, S3_PREFIX_RAW, BASE_URL, TIMEOUT_S, etc.


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def _parse_date(val: str | None, default: date) -> date:
    if not val:
        return default
    return datetime.strptime(val, "%Y-%m-%d").date()


def _media_key(prefix: str, day: date, media_id: str) -> str:
    # raw/wistia/media/dt=YYYY-MM-DD/media_id=<id>/object.json
    return f"{prefix.rstrip('/')}/media/dt={day.isoformat()}/media_id={media_id}/object.json"


def _get_secret_dict_by_name(name: str) -> Dict[str, Any]:
    """Return {} if name not set. If secret string is JSON, parse; else return {"value": "<raw>"}."""
    if not name:
        return {}
    sm = boto3.client("secretsmanager")
    try:
        resp = sm.get_secret_value(SecretId=name)
    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        raise RuntimeError(f"Failed to read secret {name}: {msg}") from e
    secret = (resp.get("SecretString") or "").strip()
    if not secret:
        return {}
    try:
        return json.loads(secret)
    except json.JSONDecodeError:
        return {"value": secret}


def _load_wistia_creds_and_ids() -> Dict[str, Any]:
    """
    Priority:
    - If WISTIA_SECRET_NAME present:
        * Parse JSON and look for 'WISTIA_API_TOKEN' or 'api_token'
        * Optionally look for 'MEDIA_IDS'/'media_ids'
        * If secret is raw token, it's under 'value'
    - If MEDIA_IDS_SECRET_NAME present:
        * Merge IDs from there (string CSV or JSON array)
    - Fallback to env WISTIA_API_TOKEN / MEDIA_IDS
    """
    token = None
    media_ids: List[str] = []

    sec_name = os.getenv("WISTIA_SECRET_NAME", "")
    if sec_name:
        obj = _get_secret_dict_by_name(sec_name)
        token = (
            obj.get("WISTIA_API_TOKEN")
            or obj.get("api_token")
            or obj.get("value")
            or token
        )
        ids_val = obj.get("MEDIA_IDS") or obj.get("media_ids")
        if isinstance(ids_val, str):
            media_ids.extend([x.strip() for x in ids_val.split(",") if x.strip()])
        elif isinstance(ids_val, list):
            media_ids.extend([str(x).strip() for x in ids_val if str(x).strip()])

    ids_name = os.getenv("MEDIA_IDS_SECRET_NAME", "")
    if ids_name:
        obj_ids = _get_secret_dict_by_name(ids_name)
        ids_val = (
            obj_ids.get("MEDIA_IDS") or obj_ids.get("media_ids") or obj_ids.get("value")
        )
        if isinstance(ids_val, str):
            media_ids.extend([x.strip() for x in ids_val.split(",") if x.strip()])
        elif isinstance(ids_val, list):
            media_ids.extend([str(x).strip() for x in ids_val if str(x).strip()])

    # Env fallbacks
    token = token or os.getenv("WISTIA_API_TOKEN")
    env_ids = os.getenv("MEDIA_IDS", "")
    if env_ids:
        media_ids.extend([x.strip() for x in env_ids.split(",") if x.strip()])

    # Dedup IDs
    media_ids = sorted(set(media_ids))

    if not token:
        raise RuntimeError("Missing WISTIA_API_TOKEN (via WISTIA_SECRET_NAME or env).")
    return {"api_token": token, "media_ids": media_ids}


def handler(event, context):
    """
    Event (Step Functions/manual):
    {
      "day": "YYYY-MM-DD"      # optional; defaults to today UTC
      "media_ids": ["abc"]     # optional; defaults to secret/env MEDIA_IDS
    }
    Writes one JSON object per media:
      s3://<S3_BUCKET_RAW>/<S3_PREFIX_RAW>/media/dt=YYYY-MM-DD/media_id=<id>/object.json
    """
    creds = _load_wistia_creds_and_ids()
    cfg = Settings.from_env()

    s3 = boto3.client("s3")

    target_day = _parse_date(
        event.get("day") if isinstance(event, dict) else None, _today_utc()
    )
    event_ids = event.get("media_ids") if isinstance(event, dict) else None
    media_ids: List[str] = event_ids or creds.get("media_ids") or []

    client = WistiaClient(
        base_url=cfg.base_url,  # e.g. https://api.wistia.com/v1
        token=creds["api_token"],
        timeout_s=cfg.request_timeout_s,
    )

    summary = {"day": target_day.isoformat(), "media": []}

    for mid in media_ids:
        data = client.medias_get(media_id=mid)

        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        key = _media_key(cfg.s3_prefix_raw, target_day, mid)

        s3.put_object(Bucket=cfg.s3_bucket_raw, Key=key, Body=body)

        summary["media"].append({"media_id": mid, "bytes": len(body)})

    return summary
