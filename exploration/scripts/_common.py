# exploration/scripts/_common.py
from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

# Prefer project client if available
try:
    from http.client import WistiaClient  # type: ignore
except Exception:  # pragma: no cover
    # Fallback minimal client if import path differs during early scaffolding
    import requests

    @dataclass
    class WistiaClient:  # type: ignore
        base_url: str
        token: str
        timeout: float = 30.0

        def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
            url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
            headers = {"Authorization": f"Bearer {self.token}"}
            r = requests.get(
                url, headers=headers, params=params or {}, timeout=self.timeout
            )
            r.raise_for_status()
            return r.json()

        def media_stats(
            self, media_id: str, params: Optional[Dict[str, Any]] = None
        ) -> Any:
            return self.get(f"stats/medias/{media_id}.json", params=params or {})

        def media_visitors(
            self, media_id: str, params: Optional[Dict[str, Any]] = None
        ) -> Any:
            return self.get(
                f"stats/medias/{media_id}/visitors.json", params=params or {}
            )

        def visitors(self, params: Optional[Dict[str, Any]] = None):
            return self.get("stats/visitors.json", params=params or {})


def _ts_for_filename() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_outdir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def write_json(obj: Any, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def load_env() -> None:
    # loads .env if present; no-op otherwise
    load_dotenv(override=False)


def env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(name, default)


def parse_media_ids(cli_value: Optional[str]) -> List[str]:
    """
    Resolve media IDs from:
      1) --media-id cli (comma-separated or repeated)
      2) MEDIA_IDS in .env (comma-separated) or MEDIA_ID (single)
    """
    if cli_value:
        # allow repeated values or comma-separated
        parts: List[str] = []
        for token in cli_value.split(","):
            token = token.strip()
            if token:
                parts.append(token)
        return parts

    env_ids = env("MEDIA_IDS") or env("MEDIA_ID")
    if env_ids:
        return [x.strip() for x in env_ids.split(",") if x.strip()]

    raise SystemExit(
        "No media id provided. Use --media-id abc123[,def456] or set MEDIA_IDS / MEDIA_ID in .env"
    )


def make_client() -> WistiaClient:
    base_url = env("WISTIA_BASE_URL", "https://api.wistia.com/v1")
    token = env("WISTIA_API_TOKEN")
    if not token:
        raise SystemExit("Missing WISTIA_API_TOKEN in environment/.env")
    timeout = float(env("WISTIA_TIMEOUT", "30"))
    return WistiaClient(base_url=base_url, token=token, timeout=timeout)


def add_common_pagination_args(ap: argparse.ArgumentParser) -> None:
    ap.add_argument(
        "--page",
        type=int,
        default=int(env("WISTIA_PAGE", "1")),
        help="Page number (default from .env or 1)",
    )
    ap.add_argument(
        "--per-page",
        type=int,
        default=int(env("WISTIA_PER_PAGE", "5")),
        help="Items per page (default from .env or 5)",
    )
    ap.add_argument(
        "--outdir",
        type=str,
        default="exploration/samples",
        help="Output directory for saved JSON files",
    )
