from __future__ import annotations
import json
from typing import Any, Dict, Iterable


def _to_jsonl(rows: Iterable[Dict[str, Any]]) -> bytes:
    return ("\n".join(json.dumps(r, ensure_ascii=False) for r in rows)).encode("utf-8")


def put_jsonl_lines(s3, *, bucket: str, key: str, rows: list[dict]) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=_to_jsonl(rows))
