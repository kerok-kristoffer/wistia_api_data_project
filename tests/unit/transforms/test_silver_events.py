# tests/transforms/test_silver_events.py
import json
import pytest
from pathlib import Path
from transforms.jobs.silver_events import main as run_job


def _write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


@pytest.mark.spark
def test_silver_events_hashes_ip_and_partitions(tmp_path, spark, monkeypatch):
    day = "2025-09-06"
    base = Path(tmp_path)

    # Simulate RAW layout on local fs
    raw_root = base / "raw" / "wistia" / "events" / f"dt={day}" / "media_id=abc123"
    _write_jsonl(
        raw_root / "page=0001.jsonl",
        [
            {
                "at": "2025-09-06T12:34:56Z",
                "event_type": "play",
                "media_id": "abc123",
                "ip": "1.2.3.4",
                "position_s": 0.0,
            },
            {
                "at": "2025-09-06T12:35:10Z",
                "event_type": "pause",
                "media_id": "abc123",
                "ip": "1.2.3.4",
                "position_s": 14.0,
            },
        ],
    )

    in_uri = f"file://{base}/raw/wistia"
    out_uri = f"file://{base}/silver/wistia"

    # Run job
    run_job(
        [
            "--input-uri",
            in_uri,
            "--output-uri",
            out_uri,
            "--day",
            day,
            "--ip-hash-key",
            "testsecret",
        ]
    )

    df = spark.read.parquet(f"{out_uri}/events")
    # partition columns exist
    assert set(["dt", "media_id"]).issubset(df.columns)
    assert df.select("dt").distinct().collect()[0][0] == day
    assert df.select("media_id").distinct().collect()[0][0] == "abc123"

    # privacy: raw ip dropped, hmac present
    assert "ip" not in df.columns
    assert "ip_hmac" in df.columns
    hashes = [r[0] for r in df.select("ip_hmac").distinct().collect()]
    assert len(hashes) == 1 and hashes[0] and len(hashes[0]) == 64

    # basic fields preserved
    assert set(["at", "event_type", "position_s"]).issubset(df.columns)
    assert df.count() == 2


@pytest.mark.spark
def test_silver_events_no_input_is_non_fatal(tmp_path, spark, capsys):
    # Day with no raw files
    day = "2025-09-07"
    base = Path(tmp_path)
    in_uri = f"file://{base}/raw/wistia"
    out_uri = f"file://{base}/silver/wistia"

    run_job(["--input-uri", in_uri, "--output-uri", out_uri, "--day", day])

    captured = capsys.readouterr().out
    assert "No input found" in captured
