from __future__ import annotations
from typing import Callable, Dict, List, Any


def fetch_all_pages(
    fetch_page_fn: Callable[[int], List[Dict[str, Any]]],
    *,
    per_page: int,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    page = 1
    while True:
        batch = fetch_page_fn(page)
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < per_page:
            break
        page += 1
    return rows
