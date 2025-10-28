import json
from pathlib import Path
from pyspark.sql.types import DateType
from transforms.jobs.silver_media_by_date import main as run_job


def _write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def test_silver_media_by_date_typing_and_partitioning(tmp_path, spark):
    day = "2025-09-06"
    base = Path(tmp_path)

    raw_root = (
        base / "raw" / "wistia" / "media_by_date" / f"dt={day}" / "media_id=abc123"
    )
    _write_jsonl(
        raw_root / "page=0001.jsonl",
        [
            {
                "date": day,
                "load_count": 21,
                "play_count": 4,
                "play_rate": 0.19,
                "hours_watched": 0.7,
                "engagement": 0.47,
                "visitors": 5,
                "media_id": "abc123",  # your ingest adds this
            }
        ],
    )

    in_uri = f"file://{base}/raw/wistia"
    out_uri = f"file://{base}/silver/wistia"

    run_job(
        [
            "--input-uri",
            in_uri,
            "--output-uri",
            out_uri,
            "--day",
            day,
        ]
    )

    df = spark.read.parquet(f"{out_uri}/media_by_date")
    # partition columns present
    assert set(["dt", "media_id"]).issubset(df.columns)
    assert df.selectExpr("CAST(dt AS STRING) AS dt").distinct().collect()[0][0] == day
    assert df.select("media_id").distinct().collect()[0][0] == "abc123"

    # schema typing
    assert "day" in df.columns
    assert isinstance(df.schema["day"].dataType, DateType)
    # core KPI fields exist
    for col in [
        "load_count",
        "play_count",
        "play_rate",
        "hours_watched",
        "engagement",
        "visitors",
    ]:
        assert col in df.columns

    row = df.collect()[0].asDict()
    assert row["day"].strftime("%Y-%m-%d") == day
    assert row["play_count"] == 4
    assert abs(row["play_rate"] - 0.19) < 1e-9


def test_silver_media_by_date_no_input_is_non_fatal(tmp_path, spark, capsys):
    day = "2025-09-07"
    base = Path(tmp_path)
    in_uri = f"file://{base}/raw/wistia"
    out_uri = f"file://{base}/silver/wistia"

    run_job(
        [
            "--input-uri",
            in_uri,
            "--output-uri",
            out_uri,
            "--day",
            day,
        ]
    )

    captured = capsys.readouterr().out
    assert "No input found" in captured
