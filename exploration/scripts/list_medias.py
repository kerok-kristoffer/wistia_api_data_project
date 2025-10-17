# exploration/scripts/list_medias.py
from __future__ import annotations

import argparse

from _common import (
    _ts_for_filename,
    add_common_pagination_args,
    ensure_outdir,
    load_env,
    make_client,
    write_json,
)


def main() -> None:
    load_env()
    ap = argparse.ArgumentParser(description="List medias to inventory basic metadata.")
    add_common_pagination_args(ap)
    args = ap.parse_args()

    client = make_client()
    # Wistia main API supports listing medias at /medias.json
    payload = client.get(
        "medias.json", params={"page": args.page, "per_page": args.per_page}
    )

    outdir = ensure_outdir(args.outdir)
    out_path = outdir / f"medias_index_p{args.page}_{_ts_for_filename()}.json"
    write_json(payload, out_path)
    print(f"Saved medias page to: {out_path}")

    # Optional: print a tiny summary
    items = payload if isinstance(payload, list) else payload.get("items") or payload
    try:
        count = len(items)
    except Exception:
        count = 1
    print(f"Items on this page: {count}")


if __name__ == "__main__":
    main()
