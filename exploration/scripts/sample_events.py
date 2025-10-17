#!/usr/bin/env python
"""
Sample the Wistia Stats Events feed.

- Fetches /v1/stats/events (account-wide), with page/per_page.
- Optional client-side filters: media_id, received_at window.
- Prints quick summaries AND saves a redacted JSON sample file.

Usage examples:
  python exploration/scripts/sample_events.py --per-page 50 --max-pages 2
  python exploration/scripts/sample_events.py --filter-media-id <MID> --per-page 50 --max-pages 4
  python exploration/scripts/sample_events.py --since 2025-10-01T00:00:00+00:00 --until 2025-10-18T00:00:00+00:00
  python exploration/scripts/sample_events.py --out exploration/samples/events_sample.json --save-limit 150
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

API_BASE = "https://api.wistia.com/v1"


def auth_headers() -> Dict[str, str]:
    load_dotenv()
    token = os.getenv("WISTIA_API_TOKEN")
    if not token:
        print("ERROR: WISTIA_API_TOKEN not set in environment/.env", file=sys.stderr)
        sys.exit(2)
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def iso_to_dt(s: str) -> dt.datetime:
    # Wistia returns e.g. "2025-10-16T13:08:36.380Z"
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


def fetch_events_page(
    page: int, per_page: int, timeout: int = 30
) -> List[Dict[str, Any]]:
    url = f"{API_BASE}/stats/events"
    params = {"page": page, "per_page": per_page}
    backoff = 1.0
    for attempt in range(3):
        try:
            r = requests.get(
                url, headers=auth_headers(), params=params, timeout=timeout
            )
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list):
                raise ValueError("Unexpected response shape (expected a JSON array).")
            return data
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
            if attempt == 2:
                raise
            time.sleep(backoff)
            backoff *= 2
    return []


def within_window(
    ev: Dict[str, Any], start: Optional[dt.datetime], end: Optional[dt.datetime]
) -> bool:
    if not start and not end:
        return True
    ra = ev.get("received_at")
    if not ra:
        return False
    t = iso_to_dt(ra)
    if start and t < start:
        return False
    if end and t >= end:
        return False
    return True


def redact_event(ev: Dict[str, Any]) -> Dict[str, Any]:
    redacted = dict(ev)
    for k in ["ip", "email", "org", "city", "lat", "lon"]:
        if k in redacted:
            redacted[k] = "<redacted>"
    return redacted


def default_out_path() -> Path:
    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return Path("exploration/samples") / f"events_sample_{ts}.json"


def main():
    ap = argparse.ArgumentParser(
        description="Sample Wistia stats events (account-wide)."
    )
    ap.add_argument(
        "--per-page",
        type=int,
        default=50,
        help="Rows per page to request (try 25–100).",
    )
    ap.add_argument(
        "--max-pages",
        type=int,
        default=2,
        help="Max pages to fetch (keep small at first).",
    )
    ap.add_argument(
        "--filter-media-id",
        type=str,
        default=None,
        help="Client-side filter on event.media_id.",
    )
    ap.add_argument(
        "--since",
        type=str,
        default=None,
        help="ISO datetime (UTC) lower bound for received_at.",
    )
    ap.add_argument(
        "--until",
        type=str,
        default=None,
        help="ISO datetime (UTC) upper bound (exclusive) for received_at.",
    )
    ap.add_argument(
        "--out",
        type=str,
        default=None,
        help="Output JSON path (default: exploration/samples/<timestamped>.json)",
    )
    ap.add_argument(
        "--save-limit",
        type=int,
        default=200,
        help="Max number of events to write to file.",
    )
    args = ap.parse_args()

    start = dt.datetime.fromisoformat(args.since) if args.since else None
    end = dt.datetime.fromisoformat(args.until) if args.until else None

    collected: List[Dict[str, Any]] = []
    for p in range(1, args.max_pages + 1):
        chunk = fetch_events_page(page=p, per_page=args.per_page)
        if not chunk:
            break
        for ev in chunk:
            if args.filter_media_id and ev.get("media_id") != args.filter_media_id:
                continue
            if not within_window(ev, start, end):
                continue
            collected.append(ev)
        if len(chunk) < args.per_page:
            break

    # --- Summaries ---
    print(f"\nFetched {len(collected)} events (after client-side filtering).")

    by_media: Dict[str, int] = {}
    for ev in collected:
        mid = ev.get("media_id") or "<none>"
        by_media[mid] = by_media.get(mid, 0) + 1
    top_media = sorted(by_media.items(), key=lambda kv: kv[1], reverse=True)[:10]
    print("\nTop media_ids by event count (top 10):")
    for mid, cnt in top_media:
        print(f"  {mid}: {cnt}")

    print("\nSample events (first 3, redacted sensitive fields):")
    samples_preview = [redact_event(ev) for ev in collected[:3]]
    print(json.dumps(samples_preview, indent=2))

    # --- Persist a redacted sample file ---
    out_path = Path(args.out) if args.out else default_out_path()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    to_save = [redact_event(ev) for ev in collected[: args.save_limit]]
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(to_save, f, indent=2, ensure_ascii=False)

    print(f"\nSaved {len(to_save)} redacted events to: {out_path.resolve()}")


if __name__ == "__main__":
    main()
