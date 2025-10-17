# Wistia API — Phase 2 Exploration Scripts

Time-boxed exploration to de-risk FR2–FR7 (auth, pagination, visitors, incremental). These scripts **do not** feed the production pipeline; they produce sample JSON and short summaries for design decisions.

> All scripts load config from `.env` via `python-dotenv`.  
> Required: `WISTIA_API_TOKEN`. Optional defaults: `WISTIA_BASE_URL`, `MEDIA_IDS`, `WISTIA_PAGE`, `WISTIA_PER_PAGE`.

---

## Common CLI Patterns

- **Windows PowerShell**
  ```powershell
  python .\exploration\scripts\<script>.py --media-id abc123
  python .\exploration\scripts\<script>.py --media-id abc123,def456 --per-page 10 --page 2
  ```

- **CMD**
  ```cmd
  python exploration\scripts\<script>.py --media-id abc123
  ```

- **macOS/Linux**
  ```bash
  python exploration/scripts/<script>.py --media-id abc123
  ```

- **PyCharm Run/Debug**
  - Script path: `exploration/scripts/<script>.py`
  - Parameters: e.g. `--media-id abc123`
  - Working directory: project root

Outputs are written under `exploration/samples/` with timestamped filenames.

---

## 1) List Medias — `list_medias.py`

**Purpose:** Inventory medias and confirm base metadata (`id`, `hashed_id`, `title`, `created_at`, …).

**Endpoint (assumed):**
```
GET /medias.json?page=<n>&per_page=<m>
```

**Run:**
```bash
python exploration/scripts/list_medias.py --page 1 --per-page 10
```

**Output:**
- JSON: `exploration/samples/medias_index_p1_<UTC_TS>.json`
- Console prints item count for a quick sense of volume.

**Use in design:**
- Select which medias to track.
- Map fields to `dim_media`.

---

## 2) Page Walk (Media Stats) — `page_walk_media_stats.py`

**Purpose:** Confirm **pagination** behavior (param names, terminal condition) and page sizes for `stats/medias`.

**Endpoint (per sample):**
```
GET /stats/medias/{MEDIA_ID}.json?page=<n>&per_page=<m>
```

**Run:**
```bash
python exploration/scripts/page_walk_media_stats.py --media-id abc123 --per-page 25 --max-pages 10
```

**Output:**
- One JSON file per page: `exploration/samples/media_{MEDIA_ID}_stats_p{n}_<UTC_TS>.json`
- Console summary: saved pages and approx item count.

**Use in design:**
- Confirms paging semantics (stop when empty).
- Provides rough volume per media/day for partitioning and concurrency choices.

---

## 3) Sample Visitors — `sample_visitors.py`

**Purpose:** Inspect **visitor-level** payloads to define `dim_visitor` and `fact_media_engagement`.

**Endpoint (assumed):**
```
GET /stats/medias/{MEDIA_ID}/visitors.json?page=<n>&per_page=<m>
```

**Run:**
```bash
python exploration/scripts/sample_visitors.py --media-id abc123 --per-page 5
```

**Output:**
- JSON: `exploration/samples/media_{MEDIA_ID}_visitors_p{n}_<UTC_TS>.json`
- Console peek: first record’s keys (helps spot IP/country/event fields).

**Use in design:**
- Validate presence/format of visitor identifiers and event metrics.
- Note any privacy-sensitive fields to redact before committing samples.

---

## 4) Incremental Probe — `incremental_probe.py`

**Purpose:** Identify a reliable **watermark** field across visitor payloads (e.g., `updated_at`), plus min/max values on small samples.

**Approach:**
- Fetch a few visitor pages (configurable).
- Scan each object (and `items[]` if wrapped) for timestamp candidates:
  ```
  updated_at, updated, modified_at, modified, created_at, created
  ```
- Summarize the count of timestamps, detected min/max (UTC), and a suggested field.

**Run:**
```bash
python exploration/scripts/incremental_probe.py --media-id abc123 --per-page 10 --max-pages 3
```

**Output:**
- Page JSON files saved under `exploration/samples/`
- Summary: `exploration/samples/incremental_probe_summary_<UTC_TS>.json` with:
  - `pages_saved`
  - `num_ts_values_found`
  - `min_timestamp` / `max_timestamp`
  - `suggested_watermark_field` (heuristic)

**Use in design:**
- Choose **the** watermark (field + timezone + format).
- Document in `docs/api_exploration.md`.

---

## Redaction & Commit Policy

- Do **not** commit secrets or raw PII.
- If visitor payloads contain IPs or sensitive fields, either:
  - Don’t commit those sample files, or
  - Redact values before committing (retain structure).

---

## Exit Criteria (move to pipeline)

- ✅ Auth & base URL confirmed
- ✅ Pagination param names and terminal condition known
- ✅ Fields present for media stats and visitors
- ✅ Watermark field decided (with example values)
- ✅ Observed rate-limit behavior (headers or 429)
- ✅ Approximate daily volumes estimated

Once the above are captured, proceed to implement the **vertical slice** (EventBridge → Step Functions → Lambda (ingest to Bronze) → Glue (Bronze→Silver) → Athena sanity).
