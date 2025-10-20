# Scope Update, Design Decisions, and Plan

This document captures the change in scope after coaching feedback, the resulting architecture choices, and a concrete plan for moving forward with implementation.

---

## Scope update (from coach)

- **No historical backfill** is required.
- We will track **exactly two media IDs** for a **7-day production run**.
- We do not need the `/stats/visitors` endpoint for our current reporting needs because **events already include geography** (e.g., `country`, `region`, `city`).

**Implication:** The ingestion layer becomes much simpler. We filter events by those two media IDs and collect daily windows only; we also fetch daily media-by-date stats for those same media.

---

## Final ingestion scope

For each of the two media IDs, **once per day**:
1. **Events** — `GET /v1/stats/events?media_id=<id>&start_date=<d>&end_date=<d>&per_page=100&page=N`
2. **Media-by-date** — `GET /v1/stats/medias/{mediaId}/by_date?start_date=<d>&end_date=<d>`
3. **(Optional) Media metadata** — `GET /v1/medias/{id}.json` (keeps `dim_media` fresh; trivial footprint)

We’ll use a **UTC daily window** (yesterday by default) and maintain watermarks per dataset **and** media ID.

---

## Architecture (updated)

**Orchestration**
- **Amazon EventBridge** cron → **AWS Step Functions**
- Step Functions **Map state** over the two media IDs (fan-out = 2)
- For each media:
  - **Lambda: ingest events**
  - **Lambda: ingest media-by-date**
  - (Optional) **Lambda: refresh media metadata**
- On success: **advance watermark** in a checkpoint store

**Checkpointing**
- **DynamoDB** table for watermarks:
  - `pk`: dataset (`"events"` or `"media_by_date"`)
  - `sk`: `media_id`
  - attributes: `last_date` (UTC), `updated_at`
- **1-day overlap**: re-ingest the previous day; we dedup in Spark on `event_key`

**Storage layout**
- **Bronze (raw JSON/NDJSON):**
  - `s3://…/bronze/wistia/events/dt=YYYY-MM-DD/media_id=<id>/part-*.json`
  - `s3://…/bronze/wistia/media_by_date/dt=YYYY-MM-DD/media_id=<id>/part-*.json`
  - `s3://…/bronze/wistia/media_metadata/media_id=<id>/load_ts=.../doc.json` (optional)
- **Silver (clean, typed, deduped):**
  - `fact_events` (partition by `dt`, optional `media_id`) — hash/drop PII, keep geo and UA
  - `fact_media_by_date` (partition by `dt`, `media_id`)
  - `dim_media` (tiny, unpartitioned, Type‑1 upserts)
- **Athena/Glue Catalog** for downstream queries/dashboards

**PII handling**
- Drop or hash PII in transformation (e.g., `ip`, `email`, raw `lat/lon`)
- Keep only `country/region` and non-sensitive attributes needed for reporting

**Reliability**
- Retries with backoff on `429`/`5xx`
- Idempotency via unique filenames in Bronze + **dedup by `event_key`** in Spark

---

## Data model (minimal viable)

- **dim_media**: `media_id`, `title`, `url`, `channel`, `created_at` (2 rows total)  
- **fact_events** (daily partition): `event_key`, `media_id`, `received_at`, `percent_viewed`, `country`, `region`, `city`, UA fields, etc.  
- **fact_media_by_date** (daily partition): `media_id`, `date`, and metrics exposed by the by_date endpoint.

> A dedicated `dim_visitor` is **deferred**. If required later, we can derive country-based analyses from events directly without storing PII.

---

## CI/CD & TDD

- **GitHub Actions** for PR-based CI:
  - Linting (`ruff`, `black`), unit tests (including HTTP client with response mocks)
  - Contract tests disabled by default; enable with repo variable/secrets when ready
- **Unit tests** for:
  - HTTP client: auth headers, retries, error mapping
  - Watermark manager (DDB): read/advance logic (mocked or moto-style)
  - Spark transforms: schema, dedup (by `event_key`), PII drop, partitioning
- **Integration tests** (thin):
  - Minimal fixture of Bronze → verify Silver outputs
- **Release**: infra templates kept in repo; deploy via PRs

---

## Runbook (daily)
1. EventBridge triggers Step Function at **02:15 UTC** (configurable)
2. For each media:
   - Read watermark for `events:<media_id>` and `media_by_date:<media_id>`
   - Ingest the **yesterday** window (with 1-day overlap)
   - Write Bronze
3. Glue job runs:
   - Read Bronze for that `dt`
   - Clean/standardize, **dedup on `event_key`**, drop PII
   - Write Silver + repair/add partitions
4. Update watermarks to `yesterday` on success

---

## Out-of-scope (by design)

- No catalog discovery (we have exactly two media)
- No historical backfill beyond the 7-day run
- No visitors API usage (unless requirements change)
- No DBT (per project constraint)

---

## Next steps (checklist)

- [ ] Finalize two media IDs as configuration (env/param/constant)
- [ ] Implement **ingest_events** Lambda (unit-tested)
- [ ] Implement **ingest_media_by_date** Lambda (unit-tested)
- [ ] Provision DynamoDB watermark table + helper (unit-tested)
- [ ] Implement Glue Spark job for Bronze→Silver (unit/integration tests)
- [ ] Create Athena DDLs; run a couple of smoke queries
- [ ] Turn on the daily schedule; monitor first 7-day run
- [ ] Document operations (how to re-run a missed day, how to override watermark)

---

## Success criteria

- Pipeline runs for **7 consecutive days** without manual intervention
- **No duplicate events** in Silver (verified by `event_key` uniqueness checks)
- PII is **not stored** beyond Bronze (and dropped in Silver)
- CI is green on every PR to `master` (lint + unit tests)
- Minimal, reproducible setup documented in the repo

---

## Configuration reference

- **Secrets**: `WISTIA_API_TOKEN`
- **Env (optional defaults)**:
  - `WISTIA_BASE_URL=https://api.wistia.com/v1`
  - `WISTIA_TIMEOUT=30`
  ️- `MEDIA_IDS=<redacted>,<redacted>`
  - `WISTIA_PER_PAGE=100`
  - `WISTIA_PAGE=1`
  - `BRONZE_BUCKET=...` / `SILVER_BUCKET=...` (used by Lambda/Glue)
  - `DDB_TABLE=WistiaWatermarks`
