#!/usr/bin/env python
# exploration/scripts/media_catalog_sweep.py
from __future__ import annotations

import argparse
import json
import time
from typing import Any, Dict, List

# Use the shared helpers + client
from _common import (
    _ts_for_filename,
    ensure_outdir,
    load_env,
    make_client,
    write_json,
)

try:
    # just to catch HTTPError for 429 handling
    import requests
    from requests import HTTPError
except Exception:  # pragma: no cover
    requests = None
    HTTPError = Exception  # type: ignore


SENSITIVE_KEYS = {"email"}  # expand as needed when we see PII in medias


def _redact(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {
            k: ("<redacted>" if k in SENSITIVE_KEYS else _redact(v))
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_redact(v) for v in obj]
    return obj


def fetch_medias(
    client,
    search: str | None,
    per_page: int,
    max_pages: int,
    sort_by: str | None,
    sort_direction: str | None,
    media_type: str | None,
    start_page: int,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Calls /v1/medias.json with page/per_page pagination, accumulating results.
    Uses the project WistiaClient from _common.make_client().
    """
    results: List[Dict[str, Any]] = []
    page = max(start_page, 1)

    for _ in range(max_pages):
        params: Dict[str, Any] = {"per_page": per_page, "page": page}
        if search:
            params["search"] = search
        if sort_by:
            params["sort_by"] = sort_by
        if sort_direction:
            params["sort_direction"] = sort_direction
        if media_type:
            params["type"] = media_type  # e.g., "Video"

        try:
            chunk = client.get("medias.json", params=params)  # <- uses base_url + token
        except HTTPError as e:  # type: ignore[misc]
            # Handle 429 politely if requests is available
            if (
                requests is not None
                and hasattr(e, "response")
                and e.response is not None
                and e.response.status_code == 429
            ):  # type: ignore[attr-defined]
                retry = int(e.response.headers.get("Retry-After", "3"))
                time.sleep(retry)
                continue
            raise

        if not chunk:
            break

        if isinstance(chunk, list):
            results.extend(chunk)
            if len(chunk) < per_page:
                break
        else:
            # Unexpected shape (dict?). Be conservative: stop paging.
            # Still include the first page content if present.
            if isinstance(chunk, dict):
                results.extend([chunk])
            break

        page += 1

    return results


def summarize(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    titles = [i.get("name") or i.get("title") or "" for i in items]
    top_preview = [t for t in titles if t][:10]
    return {"count": len(items), "top_titles": top_preview}


def main() -> None:
    load_env()

    ap = argparse.ArgumentParser(
        description="Sweep media catalog with pagination and optional search."
    )
    ap.add_argument(
        "--search",
        type=str,
        default=None,
        help="Optional free-text search (name/title).",
    )
    ap.add_argument("--per-page", type=int, default=50)
    ap.add_argument("--max-pages", type=int, default=2)
    ap.add_argument("--sort-by", type=str, default=None, help='e.g., "created"')
    ap.add_argument("--sort-direction", type=str, default=None, help='e.g., "desc"')
    ap.add_argument(
        "--type", dest="media_type", type=str, default=None, help='e.g., "Video"'
    )
    ap.add_argument("--page", type=int, default=1, help="Start page (default 1).")
    ap.add_argument(
        "--outdir",
        type=str,
        default="exploration/samples",
        help="Where to write JSON sample(s).",
    )
    args = ap.parse_args()

    client = make_client()
    items = fetch_medias(
        client=client,
        search=args.search,
        per_page=args.per_page,
        max_pages=args.max_pages,
        sort_by=args.sort_by,
        sort_direction=args.sort_direction,
        media_type=args.media_type,
        start_page=args.page,
    )

    summary = summarize(items)
    print(json.dumps(summary, indent=2))

    outdir = ensure_outdir(args.outdir)
    out_path = outdir / f"media_catalog_{_ts_for_filename()}.json"
    write_json({"summary": summary, "items": _redact(items)}, out_path)
    print(f"\nSaved sample payload: {out_path}")


if __name__ == "__main__":
    main()
