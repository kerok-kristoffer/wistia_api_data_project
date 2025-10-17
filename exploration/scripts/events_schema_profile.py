#!/usr/bin/env python
# exploration/scripts/events_schema_profile.py
from __future__ import annotations

import argparse
import collections
import json
from typing import Any, Dict, List, Tuple

from _common import (
    _ts_for_filename,
    ensure_outdir,
    load_env,
    make_client,
    write_json,
)

# Fields to redact in saved samples (PII or sensitive)
REDACT_KEYS = {"ip", "email", "lat", "lon", "city", "org"}


def _redact(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {
            k: ("<redacted>" if k in REDACT_KEYS and obj[k] is not None else _redact(v))
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_redact(v) for v in obj]
    return obj


def _infer_type(val: Any) -> str:
    if val is None:
        return "null"
    if isinstance(val, bool):
        return "bool"
    if isinstance(val, int):
        return "int"
    if isinstance(val, float):
        return "float"
    if isinstance(val, str):
        # loose ISO-8601-ish check
        if "T" in val and val.endswith("Z"):
            return "iso_datetime"
        return "str"
    if isinstance(val, dict):
        return "object"
    if isinstance(val, list):
        return "array"
    return type(val).__name__


def fetch_events(client, timeout: int = 30) -> List[Dict[str, Any]]:
    """
    Calls /v1/stats/events. Wistia returns a recent slice (no paging params documented).
    """
    items = client.get("stats/events")  # no .json in this endpoint
    if not isinstance(items, list):
        return []
    return items


def profile_schema(
    items: List[Dict[str, Any]], sample_size: int, min_percent: float | None
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Returns (sample, schema_profile)
    - sample: first N redacted events (optionally filtered by percent_viewed >= min_percent)
    - schema_profile: key -> {types, null_frac, observed_in}
    """
    if min_percent is not None:
        items = [
            e
            for e in items
            if isinstance(e.get("percent_viewed"), (int, float))
            and e["percent_viewed"] >= min_percent
        ]

    sample = items[:sample_size]

    key_counts = collections.Counter()
    type_counts: Dict[str, collections.Counter] = collections.defaultdict(
        collections.Counter
    )
    null_counts = collections.Counter()

    for e in sample:
        for k in e.keys():
            key_counts[k] += 1
            t = _infer_type(e.get(k))
            type_counts[k][t] += 1
            if e.get(k) is None:
                null_counts[k] += 1

    total = max(len(sample), 1)
    schema: Dict[str, Any] = {}
    for k in sorted(key_counts.keys()):
        schema[k] = {
            "observed_in": key_counts[k],
            "types": dict(type_counts[k]),
            "null_frac": round(null_counts[k] / total, 3),
        }

    return _redact(sample), schema


def main() -> None:
    load_env()

    ap = argparse.ArgumentParser(
        description="Profile the shape of /stats/events and save redacted samples."
    )
    ap.add_argument(
        "--sample-size",
        type=int,
        default=100,
        help="How many events to include in sample output",
    )
    ap.add_argument(
        "--min-percent",
        type=float,
        default=None,
        help="Client-side percent_viewed minimum (e.g., 0.05)",
    )
    ap.add_argument(
        "--outdir",
        type=str,
        default="exploration/samples",
        help="Where to write JSON sample(s).",
    )
    args = ap.parse_args()

    client = make_client()
    items = fetch_events(client)

    sample, schema = profile_schema(
        items, sample_size=args.sample_size, min_percent=args.min_percent
    )
    summary = {
        "fetched": len(items),
        "sampled": len(sample),
        "distinct_keys": len(schema),
        "top_keys": list(schema.keys())[:10],
    }
    print(json.dumps(summary, indent=2))

    outdir = ensure_outdir(args.outdir)
    out_path = outdir / f"events_profile_{_ts_for_filename()}.json"
    write_json({"summary": summary, "schema": schema, "sample": sample}, out_path)
    print(f"\nSaved events profile: {out_path}")


if __name__ == "__main__":
    main()
