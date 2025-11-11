import json
from pathlib import Path
from datetime import date

import pytest
from pyspark.sql import SparkSession

# Import the job under test
from transforms.jobs.silver_visitors import run_job as run_visitors_job


def _write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


@pytest.mark.spark
def test_silver_visitors_typing_partitioning_and_dedup(
    tmp_path: Path, spark: SparkSession
):
    # Arrange: RAW layout with two pages (same visitor_key, different counts)
    day = "2025-09-06"
    base = Path(tmp_path)

    raw_root = base / "raw" / "wistia" / "visitors" / f"dt={day}" / "media_id=abc123"
    _write_jsonl(
        raw_root / "page=0001.jsonl",
        [
            {
                "visitor_key": "vk-1",
                "load_count": 1,
                "play_count": 0,
                "visitor_identity": {"name": "Alice", "email": "alice@example.com"},
            },
            {
                "visitor_key": "vk-2",
                "load_count": 2,
                "play_count": 1,
                "visitor_identity": {"name": "Bob", "email": "bob@example.com"},
            },
        ],
    )
    # Duplicate visitor_key on page 2 with higher counts to test max() aggregator
    _write_jsonl(
        raw_root / "page=0002.jsonl",
        [
            {
                "visitor_key": "vk-1",
                "load_count": 3,
                "play_count": 2,
                "visitor_identity": {"name": "Alice", "email": "alice@example.com"},
            }
        ],
    )

    in_uri = f"file://{base}/raw/wistia/visitors"
    out_uri = f"file://{base}/silver/wistia/visitors"

    # Act
    run_visitors_job(
        [
            "--input-uri",
            in_uri,
            "--output-uri",
            out_uri,
            "--day",
            day,
        ]
    )

    # Assert
    df = spark.read.parquet(f"{out_uri}")
    # Partition columns exist
    assert set(["dt", "media_id"]).issubset(df.columns)

    # dt is DATE type and equals the day
    assert df.select("dt").distinct().collect()[0][0] == date.fromisoformat(day)

    # Expect dedup by visitor_key within (dt, media_id) -> two rows (vk-1 & vk-2)
    rows = {
        r["visitor_key"]: r
        for r in df.select("visitor_key", "load_count", "play_count").collect()
    }
    assert set(rows.keys()) == {"vk-1", "vk-2"}
    # vk-1 should reflect the max counts from page 2
    assert rows["vk-1"]["load_count"] == 3
    assert rows["vk-1"]["play_count"] == 2
    # vk-2 as-is
    assert rows["vk-2"]["load_count"] == 2
    assert rows["vk-2"]["play_count"] == 1

    # Basic type presence checks
    for colname in ["visitor_key", "name", "email"]:
        assert colname in df.columns
