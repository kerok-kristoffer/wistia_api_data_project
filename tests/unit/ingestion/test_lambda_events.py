from __future__ import annotations

import os

import boto3
from moto import mock_aws

from ingestion.lambda_events import handler as mod


class FakeClient:
    """Simulate 2 pages of events per media; then empty."""

    def __init__(self, *_, **__):
        self.calls = []

    def events(self, **params):
        self.calls.append(params)
        page = int(params.get("page", 1))
        if page == 1:
            return [
                {"event_key": "e1", "media_id": params["media_id"]} for _ in range(3)
            ]
        if page == 2:
            return [{"event_key": "e2", "media_id": params["media_id"]}]
        return []


@mock_aws
def test_handler_writes_partitioned_jsonl_and_summary(monkeypatch):
    # Env consumed by Settings.from_env()
    os.environ["WISTIA_BASE_URL"] = "https://api.wistia.com/v1"
    os.environ["WISTIA_API_TOKEN"] = "test-token"
    os.environ["MEDIA_IDS"] = "abc123,def456"
    os.environ["S3_BUCKET_RAW"] = "test-bkt"
    os.environ["S3_PREFIX_RAW"] = "raw/wistia"
    os.environ["DDB_TABLE_WATERMARK"] = "not-used-here"
    os.environ["REQUEST_TIMEOUT_S"] = "5"
    os.environ["PAGE_SIZE"] = "2"  # force multiple pages/files

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bkt")

    # Stub the client the handler constructs
    monkeypatch.setattr(mod, "WistiaClient", FakeClient)

    result = mod.handler({"day": "2025-01-01"}, None)
    assert result["day"] == "2025-01-01"
    counts = {m["media_id"]: m["rows"] for m in result["media"]}
    assert counts == {"abc123": 4, "def456": 4}

    # Check S3 output exists (any page files)
    r1 = s3.list_objects_v2(
        Bucket="test-bkt",
        Prefix="raw/wistia/events/dt=2025-01-01/media_id=abc123/",
    )
    keys = {o["Key"] for o in r1.get("Contents", [])}
    assert any(k.endswith("page=0001.jsonl") for k in keys)
    assert any(k.endswith("page=0002.jsonl") for k in keys)


@mock_aws
def test_fetch_all_events_stops_when_short_page(monkeypatch):
    # Small per_page and FakeClient to ensure we stop after short second page
    client = FakeClient()
    rows = mod.fetch_all_events(
        client, media_id="xyz", day=mod._today_utc(), per_page=2
    )
    # page1 returns 3 items (but we request per_page=2, still accepted as we're stubbing);
    # page2 returns 1 item; page3 returns []
    assert len(rows) == 4
