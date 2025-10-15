# Wistia API Explorer — Instructions

This repo includes a small CLI tool for **manual probing** of the Wistia Stats API and saving **golden samples** of responses for later reference and tests.

- **Script:** `exploration/scripts/explore_wistia.py`  
- **Output:** JSON files saved under `exploration/samples/` with a timestamped filename

---

## 1) One-time setup

1) Install dev dependencies (includes `python-dotenv`):

```bash
pip install -r requirements-dev.txt
```

2) Copy the example env file and fill in your values:

```bash
# macOS/Linux
cp .env-example .env
# Windows PowerShell
Copy-Item .env-example .env
```

Edit `.env`:

```dotenv
WISTIA_API_TOKEN=YOUR_TOKEN_HERE
WISTIA_BASE_URL=https://api.wistia.com/v1

# One or many (comma-separated)
MEDIA_IDS=abc123,def456

# Optional defaults
WISTIA_PAGE=1
WISTIA_PER_PAGE=5
```

---

## 2) Running the explorer (Windows + cross-platform)

You can pass media IDs on the command line **or** rely on `MEDIA_IDS` from `.env`. Command-line args take precedence.

### Windows PowerShell

```powershell
# Use IDs from .env
python .\exploration\scripts\explore_wistia.py

# Pass a single ID
python .\exploration\scripts\explore_wistia.py --media-id abc123

# Multiple IDs (comma-separated)
python .\exploration\scripts\explore_wistia.py --media-id abc123,def456 --per-page 10 --page 2
```

### Windows CMD

```cmd
python exploration\scripts\explore_wistia.py
python exploration\scripts\explore_wistia.py --media-id abc123
python exploration\scripts\explore_wistia.py --media-id abc123,def456 --per-page 10 --page 2
```

### macOS/Linux

```bash
python exploration/scripts/explore_wistia.py
python exploration/scripts/explore_wistia.py --media-id abc123
python exploration/scripts/explore_wistia.py --media-id abc123,def456 --per-page 10 --page 2
```

### PyCharm (Run/Debug)

- **Script path:** `exploration/scripts/explore_wistia.py`  
- **Parameters:** e.g. `--media-id abc123` (or leave empty to use `.env`)  
- **Working directory:** project root  
- The script auto-loads `.env` via `python-dotenv`.

---

## 3) What the script does

- Loads config from `.env` (`WISTIA_API_TOKEN`, `WISTIA_BASE_URL`, optional `MEDIA_IDS`, `WISTIA_PAGE`, `WISTIA_PER_PAGE`).
- Resolves media IDs from `--media-id` **or** `MEDIA_IDS`/`MEDIA_ID`.
- Uses the project’s `WistiaClient` to call:
  ```
  GET {WISTIA_BASE_URL}/stats/medias/{MEDIA_ID}.json
  ```
  with `page`/`per_page` query params if provided.
- Writes each response to `exploration/samples/` as:
  ```
  media_{MEDIA_ID}_p{PAGE}_{UTC_TIMESTAMP}.json
  ```
- Prints the file path for quick open/diff.

---

## 4) CLI options (help)

```bash
python exploration/scripts/explore_wistia.py -h
```

- `--media-id` : Single ID or comma-separated list. If omitted, falls back to `MEDIA_IDS` or `MEDIA_ID` from the environment/.env.  
- `--page` / `--per-page` : Pagination controls (defaults from `.env`, else 1 and 5).  
- `--outdir` : Output directory (default `exploration/samples`).  

---

## 5) Example output & quick sanity checks

**Sample response from a run:**
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

**Quick derived ratios (for your exploration notes):**
- `play_count / load_count ≈ 0.3958`
- `play_count / visitors ≈ 0.4629`

The API’s reported `play_rate` is `≈ 0.4351`, which suggests Wistia’s *play rate* definition may differ from either naive ratio. **Action:** capture this as a hypothesis in `docs/api_exploration.md` and confirm the exact definition in the official docs (e.g., unique loads vs. sessions, deduping, or time-window nuances). Also log any field semantics (are `visitors` unique users for the window? what’s the unit/base for `hours_watched`?).

---

## 6) Good exploration workflow

1. Start with a **single media ID** and small `per_page` to confirm auth & shapes.  
2. Try multiple `per_page` values (1, 5, 50) to see practical limits.  
3. Increment `--page` until the terminal condition (empty/last page) to confirm pagination semantics.  
4. Look for **incremental fields** (`updated_at`, `created_at`) and note format/timezone for watermarks.  
5. Save a few **representative responses** (scrubbed) as golden samples; later copy tiny fields into `tests/fixtures/` for robust unit tests.  
6. Record findings in `docs/api_exploration.md` (auth, pagination, rate limits, timestamp formats, edge cases).

---

## 7) Troubleshooting

- **Missing token:** `WISTIA_API_TOKEN` not set → add to `.env` or export in shell.  
- **401 Unauthorized:** invalid token or missing scope.  
- **404 Not Found:** wrong media ID or insufficient access.  
- **429 Too Many Requests:** rate-limited; back off. The client includes basic retry/backoff logic.  
- **Large responses:** keep `per_page` small during exploration—only samples are needed now.

---
