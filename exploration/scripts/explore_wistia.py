# exploration/scripts/explore_wistia.py
import argparse
import json
import os
import sys
import time
from typing import List, Optional

from dotenv import load_dotenv
from ingestion.http import WistiaClient  # your client


def _parse_media_ids(cli_value: Optional[str]) -> List[str]:
    """Resolve media IDs from CLI or env (MEDIA_IDS or MEDIA_ID)."""
    if cli_value:
        return [m.strip() for m in cli_value.split(",") if m.strip()]
    env_val = os.getenv("MEDIA_IDS") or os.getenv("MEDIA_ID")
    if env_val:
        return [m.strip() for m in env_val.split(",") if m.strip()]
    return []


def main() -> None:
    load_dotenv()  # reads .env at repo root if present

    token = os.getenv("WISTIA_API_TOKEN")
    base_url = os.getenv("WISTIA_BASE_URL", "https://api.wistia.com/v1")

    ap = argparse.ArgumentParser(description="Quick probe of Wistia stats API.")
    ap.add_argument(
        "--media-id",
        help="Single media ID or comma-separated list. "
        "Defaults to MEDIA_IDS (or MEDIA_ID) in env/.env.",
    )
    ap.add_argument("--page", type=int, default=int(os.getenv("WISTIA_PAGE", "1")))
    ap.add_argument(
        "--per-page", type=int, default=int(os.getenv("WISTIA_PER_PAGE", "5"))
    )
    ap.add_argument("--outdir", default="exploration/samples")
    args = ap.parse_args()

    ids = _parse_media_ids(args.media_id)

    if not token:
        sys.exit("Missing WISTIA_API_TOKEN (set it in .env or your environment).")
    if not ids:
        sys.exit("Provide --media-id or set MEDIA_IDS in .env / environment.")

    client = WistiaClient(base_url, token)
    os.makedirs(args.outdir, exist_ok=True)

    for mid in ids:
        data = client.media_stats(mid, page=args.page, per_page=args.per_page)
        ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        outpath = os.path.join(args.outdir, f"media_{mid}_p{args.page}_{ts}.json")
        with open(outpath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"Saved {outpath}")


if __name__ == "__main__":
    main()
