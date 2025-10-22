import json
import os

import boto3
import moto
import responses

from ingestion.lambda_media_by_date.handler import handler as media_by_date_handler

RAW_PREFIX = "raw"


@moto.mock_aws
@responses.activate
def test_media_by_date_lambda_writes_jsonl_and_returns_counts_non_empty():
    # Arrange: S3 + env
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "test-bucket"
    s3.create_bucket(Bucket=bucket)

    os.environ["S3_BUCKET_RAW"] = bucket
    os.environ["S3_PREFIX_RAW"] = RAW_PREFIX
    os.environ["WISTIA_BASE_URL"] = "https://api.wistia.com/v1"
    os.environ["WISTIA_API_TOKEN"] = "redacted"
    os.environ["MEDIA_IDS"] = "abc123,def456"

    # Mock Wistia for both media IDs for a specific day
    day = "2025-10-20"
    for mid in ["abc123", "def456"]:
        url = f"https://api.wistia.com/v1/stats/medias/{mid}/by_date"
        responses.add(
            responses.GET,
            url,
            json=[
                {
                    "date": day,
                    "load_count": 10,
                    "play_count": 3,
                    "play_rate": 0.3,
                    "hours_watched": 0.5,
                    "engagement": 0.4,
                    "visitors": 4,
                }
            ],
            status=200,
            match=[
                responses.matchers.query_param_matcher(
                    {"start_date": day, "end_date": day}
                )
            ],
        )

    # Act
    result = media_by_date_handler({"day": day}, {})

    # Assert return shape
    assert result["day"] == day
    assert {m["media_id"] for m in result["media"]} == {"abc123", "def456"}
    for m in result["media"]:
        assert m["rows"] == 1

    # Assert S3 keys & non-empty content
    for mid in ["abc123", "def456"]:
        key = (
            f"{RAW_PREFIX}/wistia/media_by_date/dt={day}/media_id={mid}/page=0001.jsonl"
        )
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode("utf-8")
        lines = [ln for ln in body.splitlines() if ln.strip()]
        assert len(lines) == 1
        row = json.loads(lines[0])
        assert row["date"] == day
        assert row["play_count"] == 3
        assert row["media_id"] == mid  # handler adds media_id into each row


@moto.mock_aws
@responses.activate
def test_media_by_date_lambda_writes_empty_file_on_empty_api():
    # Arrange
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "test-bucket"
    s3.create_bucket(Bucket=bucket)

    os.environ["S3_BUCKET_RAW"] = bucket
    os.environ["S3_PREFIX_RAW"] = RAW_PREFIX
    os.environ["WISTIA_BASE_URL"] = "https://api.wistia.com/v1"
    os.environ["WISTIA_API_TOKEN"] = "redacted"
    os.environ["MEDIA_IDS"] = "solo"

    day = "2025-10-21"
    url = "https://api.wistia.com/v1/stats/medias/solo/by_date"
    responses.add(
        responses.GET,
        url,
        json=[],
        status=200,
        match=[
            responses.matchers.query_param_matcher({"start_date": day, "end_date": day})
        ],
    )

    # Act
    result = media_by_date_handler({"day": day}, {})

    # Assert return shape
    assert result["day"] == day
    assert result["media"] == [{"media_id": "solo", "rows": 0}]

    # Assert zero-byte placeholder file exists
    key = f"{RAW_PREFIX}/wistia/media_by_date/dt={day}/media_id=solo/page=0001.jsonl"
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    assert len(body) == 0
