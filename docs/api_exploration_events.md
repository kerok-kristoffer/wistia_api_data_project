# API Exploration — Events Endpoint

_This document extends the existing API exploration notes and summarizes what we validated specifically for **Wistia Stats — Events**. It’s meant to capture what we ran, what we saw, and how these findings drive the pipeline design._

## Why explore events?
Events power most of the reporting asked for in the project (plays, engagement, geography, device/browser). We validated:
- Authentication with Bearer token
- Endpoint behavior and parameters
- Pagination and practical page size
- Presence of sensitive fields (PII) and a safe redaction approach
- A reliable **dedup key** (`event_key`)

---

## Endpoints confirmed

- **Events (global or filtered):**  
  `GET https://api.wistia.com/v1/stats/events`

  **Supported query params we used/validated:**
  - `media_id=<id>` (filter to one specific video)
  - `start_date=YYYY-MM-DD` and `end_date=YYYY-MM-DD` (UTC windowing)
  - `per_page` (capped at 100) and `page` for pagination

- **Media by date** (validated for the complementary daily rollups, not repeated here):  
  `GET https://api.wistia.com/v1/stats/medias/{mediaId}/by_date?start_date=...&end_date=...`

---

## Auth & configuration

We rely on `.env` during exploration:

```
WISTIA_API_TOKEN=***redacted***
WISTIA_BASE_URL=https://api.wistia.com/v1
WISTIA_TIMEOUT=30
WISTIA_PER_PAGE=100
WISTIA_PAGE=1
```

The exploration scripts load these automatically via `dotenv` and the shared helper `exploration/scripts/_common.py`.

---

## Script used

### `exploration/scripts/sample_events.py`

- Fetches events using `GET /v1/stats/events`.
- Supports optional filtering (`--media-id`, `--start-date`, `--end-date`) and paging (`--per-page`, `--max-pages`).
- Prints a quick summary (counts per `media_id`) and shows a few **redacted** sample events.
- Optionally saves a sample file when `--save-sample` is provided (to `exploration/samples/`).

**Examples**

```bash
# 1) Unfiltered (token scope) first page only
python exploration/scripts/sample_events.py

# 2) Filter to a specific media for a day window
python exploration/scripts/sample_events.py   --media-id ayxhuq5wcn   --start-date 2025-10-16   --end-date 2025-10-16

# 3) Pull multiple pages at size 100 (client will stop when page is empty)
python exploration/scripts/sample_events.py   --media-id asvat5wjl5   --start-date 2025-10-16   --end-date 2025-10-16   --per-page 100 --max-pages 10   --save-sample
```

**Output (abridged)**

```
Fetched 100 events (after client-side filtering).

Top media_ids by event count (top 10):
  <redacted>: 31
  <redacted>: 31
  <redacted>: 13
  <redacted>: 6
  <redacted>: 3
  <redacted>: 3
  <redacted>: 2
  <redacted>: 2
  <redacted>: 2
  <redacted>: 1

Sample events (first 3, redacted sensitive fields):
[
  {
    "received_at": "2025-10-17T17:24:53.000Z",
    "event_key": "1760721_0a81a338-...-abca",
    "ip": "<redacted>",
    "country": "US",
    "region": "Florida",
    "city": "<redacted>",
    "lat": "<redacted>",
    "lon": "<redacted>",
    "org": "<redacted>",
    "email": "<redacted>",
    "percent_viewed": 0.010824053124908178,
    "embed_url": "https://dataengineerinterviews.com/",
    "conversion_type": "",
    "conversion_data": {},
    "iframe_heatmap_url": "https://api.wistia.com/v1/stats/events/.../iframe.html?public_token=...",
    "visitor_key": "1760721_33d184e1-...-f4ea",
    "user_agent_details": {
      "browser": "Chrome",
      "browser_version": "141",
      "platform": "iOS (iPhone)",
      "mobile": true
    },
    "media_id": "ayxhuq5wcn",
    "media_name": "vsl short vid with long testimonials FB 5-27-2025",
    "media_url": "https://.../medias/ayxhuq5wcn"
  },
  ...
]
```

> **Redaction policy (exploration):** we redact `ip`, `email`, `lat`, `lon`, and any free-text org/city to avoid committing PII to the repo. In the pipeline, we will **drop** PII fields at Silver and keep only geography fields needed for reporting (`country`, `region`).

---

## Observations that affect design

- **Deduplication:** `event_key` appears unique and stable → we’ll dedup on it in Spark.
- **Geo & device:** Events include `country`, `region`, `city`, and `user_agent_details` → no need to hit the visitors endpoint for our current goals.
- **Pagination:** `per_page` allows up to 100; we loop pages until empty.
- **Windowing:** `start_date`/`end_date` lets us implement a simple daily window with a 1-day overlap.
- **PII:** We’ll drop `ip`, `email`, and raw coordinates in transformation to avoid storing sensitive personal data.

---

## Where sample files land

When `--save-sample` is provided, JSON outputs are saved under:

```
exploration/samples/events_sample_<timestamp>.json
```

These samples are for documentation and schema inspection only (not used by the production pipeline).

---

## What’s next

- Use the confirmed params for the **daily ingestion Lambdas**:
  - `GET /v1/stats/events?media_id=<id>&start_date=<d>&end_date=<d>&per_page=100&page=N`.
- Store raw results in **Bronze** and dedup/clean in **Silver**.
- Keep a **per-media watermark** to set the next day’s window (with a 1-day overlap for safety).
