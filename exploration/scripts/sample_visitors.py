# exploration/scripts/sample_visitors.py
from __future__ import annotations

import argparse
from typing import List

from _common import (
    _ts_for_filename,
    add_common_pagination_args,
    ensure_outdir,
    load_env,
    make_client,
    parse_media_ids,
    write_json,
)


def main() -> None:
    load_env()
    ap = argparse.ArgumentParser(
        description="Fetch a small slice of visitor events for given media id(s)."
    )
    ap.add_argument(
        "--media-id",
        type=str,
        help="Single or comma-separated media ids (or set MEDIA_IDS in .env)",
    )
    add_common_pagination_args(ap)
    args = ap.parse_args()

    client = make_client()
    media_ids: List[str] = parse_media_ids(args.media_id)
    outdir = ensure_outdir(args.outdir)

    for mid in media_ids:
        page = args.page
        payload = client.visitors(params={"page": page, "per_page": args.per_page})
        out_path = outdir / f"visitors_p{args.page}_{_ts_for_filename()}.json"
        write_json(payload, out_path)
        print(f"Saved visitors page to: {out_path}")

        # Print quick field peek for first item
        items = payload if isinstance(payload, list) else payload.get("items") or []
        if items:
            sample = items[0]
            print("Sample visitor keys:", list(sample.keys())[:12])


if __name__ == "__main__":
    main()
