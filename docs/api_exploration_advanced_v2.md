# Wistia API Exploration (Updated)

This document records the **exploration scripts** used to understand Wistia’s Stats API behavior for this project.  
It intentionally focuses on lightweight, **manual probes** that inform our architecture and pipeline decisions.

> ✅ **Scope update**
> - We will **defer ingesting `visitors` dimension** into the pipeline for now (avoid handling PII until needed).
> - We **continue** to explore `visitors` just enough to understand payload shape and privacy surface.
> - We **removed** the previous `incremental_probe.py` section (that script referenced a non-existent endpoint).

---

## Prerequisites

- Python 3.10+ virtual environment activated
- `.env` at project root with:
  - `WISTIA_API_TOKEN="..."` (Stats API token)
  - Optionally: `MEDIA_IDS="id1,id2"`
- Install dev dependencies:
  ```bash
  pip install -r requirements-dev.txt
  ```

> All scripts live under `exploration/scripts/` and write sample JSON/NDJSON under `exploration/samples/`.
> The CLI examples below include both **PowerShell** and **bash/zsh** variants.

---

## 1) `list_medias.py` — Inventory & Shape Check

**Goal**: Confirm authentication, enumerate medias the token can see, validate pagination, and capture fields for `dim_media` design (e.g., `id`/**hashed_id**, `title`, `created_at`, `url`, `channel`).

**Run (PowerShell)**
```powershell
python .\\exploration\\scripts\\list_medias.py `
  --page 1 `
  --per-page 25
```

**Run (bash/zsh)**
```bash
python exploration/scripts/list_medias.py \
  --page 1 \
  --per-page 25
```

**Notes**
- Use `--page` and `--per-page` to page through inventory deterministically.
- Output file (timestamped): `exploration/samples/medias/list_medias_<timestamp>.json`
- What to look for:
  - Total count (approximate), earliest/latest `created_at`
  - Channel mix (Facebook/YouTube/other)
  - Stable key to store as `media_id` (typically the **hashed_id** in Stats API contexts)

---

## 2) `page_walk_media_stats.py` — Media-Level Stats Sampler

**Goal**: Fetch **media-level aggregates** for one or more medias to validate metric names/types used later in `fact_media_engagement` (plays, play rate, watch time, etc.).

**Run (PowerShell)**
```powershell
python .\\exploration\\scripts\\page_walk_media_stats.py `
  --media-id abc123 `
  --per-page 25 `
  --max-pages 3
```

**Run (bash/zsh)**
```bash
python exploration/scripts/page_walk_media_stats.py \
  --media-id abc123 \
  --per-page 25 \
  --max-pages 3
```

> Some media-level endpoints are **aggregate-only** and may not expose traditional pagination. The script still supports `--per-page/--max-pages` for consistent CLI ergonomics and future-proofing. When pagination is unsupported, the script simply processes your media list and writes one record per media.

**Example observed output (from an early run)**
```json
{
  "load_count": 110694,
  "play_count": 43810,
  "play_rate": 0.4350761882621468,
  "hours_watched": 2651.986969632,
  "engagement": 0.500445,
  "visitors": 94634
}
```

**Notes**
- Output file (timestamped): `exploration/samples/media_stats/media_stats_<timestamp>.json`
- What to look for:
  - Field names, nullability, and numeric ranges (e.g., `play_rate` is a fraction)
  - Whether you need rounding/decimals normalization in Spark
  - Alignment with your downstream KPI definitions

---

## 3) `sample_visitors.py` — Visitor Payload & Privacy Surface (Exploration Only)

**Goal**: Inspect **visitor** payload shape to understand identifiers and any PII exposure (e.g., IP, user agent). We’re **not** using visitors in the initial pipeline, but we record structure for future reference.

**Run (PowerShell)**
```powershell
python .\\exploration\\scripts\\sample_visitors.py `
  --page 1 `
  --per-page 10
```

**Run (bash/zsh)**
```bash
python exploration/scripts/sample_visitors.py \
  --page 1 \
  --per-page 10
```

**Important**
- Endpoint is **account-scoped** (not nested under a media).
- Pass `--page/--per-page` explicitly for deterministic results (max commonly 100).
- We save a sample to help decide redaction and keying.

## Sanitized Visitors Sample (Wistia Stats API)

**Redaction rules used here**
- `visitor_key`, `last_event_key`: keep numeric prefix, mask the rest.
- `email`: shown as `null` (mask or drop if present).
- Other fields retained as-is for schema clarity.

```json
[
  {
    "visitor_key": "1760645_***redacted***",
    "created_at": "2025-10-16T20:07:48.000Z",
    "last_active_at": "2025-10-16T20:07:48.000Z",
    "last_event_key": "1760645_***redacted***",
    "load_count": 1,
    "play_count": 1,
    "identifying_event_key": null,
    "visitor_identity": {
      "name": "",
      "email": null,
      "org": {
        "name": null,
        "title": null
      }
    },
    "user_agent_details": {
      "browser": "Unknown Browser",
      "browser_version": "0",
      "platform": "iOS (iPhone)",
      "mobile": true
    }
  }
]
```

**Notes**
- Output file (timestamped): `exploration/samples/visitors/visitors_page_<n>_<timestamp>.json`
- Redaction policy for PII (proposal):
  - Either **drop** the IP entirely, or **hash** it (e.g., SHA-256 with salt) and **never store** raw IP beyond exploration.
  - Keep country/region if needed for aggregate geo KPIs.
- Since we’re not ingesting visitors now, treat this as **reference only**.

---

## What we removed

### (Removed) `incremental_probe.py`
- This script previously attempted to call a non-existent nested endpoint for visitors under a media.  
- Incremental strategy for the pipeline will be anchored on **events**, not visitors.

---

## Next Steps (toward the pipeline)

1. **Events exploration** (new script): confirm filtering by media/date, page size, and fields required to build daily facts.
2. Decide **watermark** (e.g., `occurred_at`/`updated_at`) and **late-arrival window** for events.
3. Lock down **Bronze S3 prefixes** and a minimal **Silver fact_media_day** schema.

> Once events are understood, we can implement the first vertical slice: ingest → Bronze → Spark rollup → Athena sanity query.

---

## Troubleshooting

- If you receive `401 Unauthorized`, check `WISTIA_API_TOKEN` and token scopes.
- For `404 Not Found`, verify the **endpoint path** (visitors are not under `/stats/medias/{id}`).
- If pagination seems stuck at ~10 results, pass `--per-page 100` explicitly.
- Use `--base-url` only if you’re targeting a different API host (rare).

---

## Changelog (doc)

- **2025-10-16**: Removed `incremental_probe.py`. Updated visitors section and added sanitized example. Clarified pagination and scope.