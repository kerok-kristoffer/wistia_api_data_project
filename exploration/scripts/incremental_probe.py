# exploration/scripts/incremental_probe.py
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from _common import (
    _ts_for_filename,
    add_common_pagination_args,
    ensure_outdir,
    load_env,
    make_client,
    parse_media_ids,
    write_json,
)

CANDIDATE_TS_FIELDS = (
    "updated_at",
    "updated",
    "modified_at",
    "modified",
    "created_at",
    "created",
)


def _collect_timestamps(obj: Any) -> List[str]:
    vals: List[str] = []
    if isinstance(obj, dict):
        for k in CANDIDATE_TS_FIELDS:
            if k in obj and isinstance(obj[k], str):
                vals.append(obj[k])
        # if wrapped
        items = obj.get("items")
        if isinstance(items, list):
            for it in items:
                vals.extend(_collect_timestamps(it))
    elif isinstance(obj, list):
        for it in obj:
            vals.extend(_collect_timestamps(it))
    return vals


def _iso_or_none(s: str) -> Optional[datetime]:
    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def main() -> None:
    load_env()
    ap = argparse.ArgumentParser(
        description="Probe payloads for timestamp fields suitable for incremental watermarks."
    )
    ap.add_argument(
        "--media-id",
        type=str,
        help="Single or comma-separated media ids (or set MEDIA_IDS in .env)",
    )
    add_common_pagination_args(ap)
    ap.add_argument(
        "--max-pages", type=int, default=3, help="Pages to inspect per media"
    )
    args = ap.parse_args()

    client = make_client()
    media_ids: List[str] = parse_media_ids(args.media_id)
    outdir = ensure_outdir(args.outdir)

    summary: Dict[str, Any] = {
        "probed": [],
        "candidates": CANDIDATE_TS_FIELDS,
        "media": {},
    }

    for mid in media_ids:
        page = args.page
        ts_values: List[str] = []
        saved = 0
        while page < args.page + args.max_pages:
            payload = client.media_visitors(
                mid, params={"page": page, "per_page": args.per_page}
            )
            out_file = (
                outdir / f"media_{mid}_visitors_probe_p{page}_{_ts_for_filename()}.json"
            )
            write_json(payload, out_file)
            saved += 1

            ts_values.extend(_collect_timestamps(payload))
            # simple termination criteria
            if isinstance(payload, list) and len(payload) == 0:
                break
            if (
                isinstance(payload, dict)
                and isinstance(payload.get("items"), list)
                and len(payload["items"]) == 0
            ):
                break
            # For dict-only single object responses, bail after 1 page
            if not isinstance(payload, list) and not isinstance(
                payload.get("items", []), list
            ):
                break
            page += 1

        # Parse timestamps into datetimes (UTC) and get min/max
        parsed = sorted(
            [_iso_or_none(s) for s in ts_values if _iso_or_none(s)],
            key=lambda x: x.timestamp(),
        )  # type: ignore
        mn = parsed[0].isoformat() if parsed else None
        mx = parsed[-1].isoformat() if parsed else None
        summary["media"][mid] = {
            "pages_saved": saved,
            "num_ts_values_found": len(ts_values),
            "min_timestamp": mn,
            "max_timestamp": mx,
            "suggested_watermark_field": next(
                (
                    f
                    for f in CANDIDATE_TS_FIELDS
                    if f in (ts_values and ",".join(ts_values))
                ),
                None,
            ),
        }
        summary["probed"].append(mid)
        print(f"[{mid}] pages={saved} ts_values={len(ts_values)} min={mn} max={mx}")

    summary_path = outdir / f"incremental_probe_summary_{_ts_for_filename()}.json"
    write_json(summary, summary_path)
    print(f"Saved summary to: {summary_path}")


if __name__ == "__main__":
    main()
