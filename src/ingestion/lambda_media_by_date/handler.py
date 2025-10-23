# src/ingestion/lambda_media_by_date/handler.py
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List

import boto3

from ingestion.http import WistiaClient


def _yesterday_utc() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()


def _load_config() -> Dict[str, Any]:
    """
    Load Wistia config from Secrets Manager (if WISTIA_SECRET_ARN set), falling back to env.
    No new env names are introduced; we use the exact existing ones.
    """
    cfg = {
        "base_url": os.getenv("WISTIA_BASE_URL", "https://api.wistia.com/v1"),
        "api_token": os.getenv("WISTIA_API_TOKEN"),
        "media_ids": [
            m.strip() for m in os.getenv("MEDIA_IDS", "").split(",") if m.strip()
        ],
        "s3_bucket": os.getenv("S3_BUCKET_RAW"),
        "s3_prefix": os.getenv("S3_PREFIX_RAW", "raw"),
        "secret_arn": os.getenv("WISTIA_SECRET_ARN"),
    }

    if cfg["secret_arn"]:
        try:
            sm = boto3.client("secretsmanager")
            resp = sm.get_secret_value(SecretId=cfg["secret_arn"])
            payload = resp.get("SecretString") or "{}"
            data = json.loads(payload)
            # Only override if present in secret
            if data.get("api_token"):
                cfg["api_token"] = data["api_token"]
            if data.get("media_ids"):
                # accept comma string or list
                if isinstance(data["media_ids"], str):
                    cfg["media_ids"] = [
                        m.strip() for m in data["media_ids"].split(",") if m.strip()
                    ]
                elif isinstance(data["media_ids"], list):
                    cfg["media_ids"] = [
                        str(m).strip() for m in data["media_ids"] if str(m).strip()
                    ]
        except Exception:
            # Non-fatal: fall back to env-only
            pass

    return cfg


def _s3_key(prefix: str, day: str, media_id: str) -> str:
    prefix = prefix.rstrip("/")
    return f"{prefix}/wistia/media_by_date/dt={day}/media_id={media_id}/page=0001.jsonl"


def _write_jsonl(s3, bucket: str, key: str, rows: List[dict]) -> int:
    """
    Writes JSONL. If rows is empty, writes a zero-byte object (still creates the partition key).
    Returns number of rows written.
    """
    if not rows:
        s3.put_object(Bucket=bucket, Key=key, Body=b"")
        return 0
    body = "\n".join(json.dumps({**r}) for r in rows).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    return len(rows)


def handler(event, context):
    """
    Input: { "day": "YYYY-MM-DD" } optional; defaults to UTC yesterday
    Output: { "day": "...", "media": [ { "media_id": "...", "rows": N }, ... ] }
    """
    day = (event or {}).get("day") or _yesterday_utc()

    cfg = _load_config()
    assert cfg[
        "api_token"
    ], "WISTIA_API_TOKEN missing (or Secrets Manager missing 'api_token')"
    assert cfg[
        "media_ids"
    ], "MEDIA_IDS missing (or Secrets Manager missing 'media_ids')"
    assert cfg["s3_bucket"], "S3_BUCKET_RAW missing"

    client = WistiaClient(base_url=cfg["base_url"], token=cfg["api_token"])
    s3 = boto3.client("s3")

    results = []
    for media_id in cfg["media_ids"]:
        # Fetch a single day window [day, day]
        rows = client.media_stats_by_date(
            media_id=media_id, start_date=day, end_date=day
        )
        # Ensure media_id included per line for convenience downstream
        rows = [{**r, "media_id": media_id} for r in rows]

        key = _s3_key(cfg["s3_prefix"], day, media_id)
        written = _write_jsonl(s3, cfg["s3_bucket"], key, rows)
        results.append({"media_id": media_id, "rows": written})

    return {"day": day, "media": results}
