from __future__ import annotations

import json

import boto3
from ingestion.http import WistiaClient
from ingestion.settings import Settings
from ingestion.common.dates import _today_utc, parse_iso_date
from ingestion.common.secrets import load_wistia_secret, merge_wistia_secret_into_env
from ingestion.common.s3io import put_jsonl_lines
from ingestion.common.paths import visitors_key


def handler(event, context):
    secret = load_wistia_secret()
    merge_wistia_secret_into_env(secret)

    cfg = Settings.from_env()
    target_day = parse_iso_date((event or {}).get("day"), _today_utc())
    media_ids = (event or {}).get("media_ids") or (cfg.media_ids or [])

    client = WistiaClient(
        base_url=cfg.base_url,
        token=secret["api_token"],
        timeout_s=cfg.request_timeout_s,
    )
    s3 = boto3.client("s3")
    iam = boto3.client("sts")
    print("[ROLE] CallerIdentity:", json.dumps(iam.get_caller_identity()))

    summary = {"day": target_day.isoformat(), "media": []}

    for mid in media_ids:
        rows_total = 0
        page = 1
        while True:
            batch = client.stats_visitors(media_id=mid, page=page)
            if not batch:
                break
            rows_total += len(batch)
            print(
                f"[VISITORS] Writing {len(rows_total)} rows to s3://{cfg.s3_bucket_raw}/{visitors_key(cfg.s3_prefix_raw, target_day, mid, page)}"
            )
            put_jsonl_lines(
                s3,
                bucket=cfg.s3_bucket_raw,
                key=visitors_key(cfg.s3_prefix_raw, target_day, mid, page),
                rows=batch,
            )
            if len(batch) < cfg.page_size:
                break
            page += 1

        summary["media"].append({"media_id": mid, "rows": rows_total})

    return summary
