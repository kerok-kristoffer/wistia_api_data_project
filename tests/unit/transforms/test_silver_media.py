import json
from pathlib import Path
from datetime import date

import pytest
from pyspark.sql import SparkSession

from transforms.jobs.silver_media import run_job as run_media_job


def _write_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")


@pytest.mark.spark
def test_silver_media_history_and_snapshot(tmp_path: Path, spark: SparkSession):
    day = "2025-09-06"
    base = Path(tmp_path)

    sample = {
        "id": 128842548,
        "hashed_id": "abc123",
        "name": "Sample Video",
        "duration": 435.456,
        "created": "2025-01-13T00:31:55+00:00",
        "updated": "2025-05-01T14:46:32+00:00",
        "status": "ready",
        "archived": False,
        "section": "VSLs",
        "thumbnail": {"url": "https://example/thumb.jpg", "width": 200, "height": 120},
        "project": {"id": 8017828, "name": "Main Funnel - B2C"},
        "progress": 1,
    }

    raw_obj = (
        base
        / "raw"
        / "wistia"
        / "media"
        / f"dt={day}"
        / "media_id=abc123"
        / "object.json"
    )
    _write_json(raw_obj, sample)

    in_uri = f"file://{base}/raw/wistia"
    out_uri = f"file://{base}/silver/wistia"

    # Act
    run_media_job(
        [
            "--input-uri",
            in_uri,
            "--output-uri",
            out_uri,
            "--day",
            day,
        ]
    )

    # Assert history
    hist = spark.read.parquet(f"{out_uri}/media_history")
    assert {"dt", "media_id"}.issubset(hist.columns)
    assert hist.select("dt").distinct().collect()[0][0] == date.fromisoformat(day)

    row = (
        hist.select(
            "media_id",
            "name",
            "duration_s",
            "created_at",
            "updated_at",
            "status",
            "archived",
            "section",
            "project_id",
            "project_name",
            "thumbnail_url",
            "progress",
        )
        .collect()[0]
        .asDict()
    )

    assert row["media_id"] == "abc123"
    assert row["name"] == "Sample Video"
    assert abs(row["duration_s"] - 435.456) < 1e-6
    assert str(row["created_at"]).startswith("2025-01-13")
    assert str(row["updated_at"]).startswith("2025-05-01")
    assert row["status"] == "ready"
    assert row["archived"] is False
    assert row["section"] == "VSLs"
    assert row["project_id"] == 8017828
    assert row["project_name"] == "Main Funnel - B2C"
    assert row["thumbnail_url"] == "https://example/thumb.jpg"
    assert abs(row["progress"] - 1.0) < 1e-9

    # Assert snapshot
    snap = spark.read.parquet(f"{out_uri}/media_snapshot")
    # One row per media_id (deduped)
    rows = snap.where(snap.media_id == "abc123").collect()
    assert len(rows) == 1
    assert rows[0]["name"] == "Sample Video"
