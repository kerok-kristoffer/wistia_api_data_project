# exploration/scripts/page_walk_media_stats.py
from __future__ import annotations

import argparse
from typing import Any, List

from _common import (
    _ts_for_filename,
    add_common_pagination_args,
    ensure_outdir,
    load_env,
    make_client,
    parse_media_ids,
    write_json,
)


def is_empty_page(payload: Any) -> bool:
    # Treat [] as empty; for dict responses, assume not paginated beyond page 1
    if payload is None:
        return True
    if isinstance(payload, list):
        return len(payload) == 0
    if isinstance(payload, dict):
        # Some APIs wrap items into {"items":[...]}
        items = payload.get("items")
        if isinstance(items, list):
            return len(items) == 0
        # If the stats endpoint returns an object (not items), assume single page
        return False
    return False


def main() -> None:
    load_env()
    ap = argparse.ArgumentParser(
        description="Walk through media stats pages to confirm pagination."
    )
    ap.add_argument(
        "--media-id",
        type=str,
        help="Single or comma-separated media ids (or set MEDIA_IDS in .env)",
    )
    add_common_pagination_args(ap)
    ap.add_argument(
        "--max-pages", type=int, default=25, help="Safety cap on pages to walk"
    )
    args = ap.parse_args()

    media_ids: List[str] = parse_media_ids(args.media_id)
    client = make_client()
    outdir = ensure_outdir(args.outdir)

    for mid in media_ids:
        page = args.page
        total_items = 0
        saved = 0
        print(f"\n=== Media {mid} ===")
        while page < args.page + args.max_pages:
            payload = client.media_stats(
                mid, params={"page": page, "per_page": args.per_page}
            )
            # Save each page
            out_path = outdir / f"media_{mid}_stats_p{page}_{_ts_for_filename()}.json"
            write_json(payload, out_path)
            saved += 1

            # Count items if it's a list or wrapped items
            if isinstance(payload, list):
                total_items += len(payload)
            elif isinstance(payload, dict) and isinstance(payload.get("items"), list):
                total_items += len(payload["items"])
            else:
                # Single-object response — assume no further pages
                break

            if is_empty_page(payload):
                break
            page += 1

        print(f"Saved {saved} page file(s). Approx items: {total_items}")


if __name__ == "__main__":
    main()
