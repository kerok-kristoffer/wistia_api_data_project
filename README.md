# Wistia Video Analytics – Data Engineering Pipeline

> **Status:** Planning & scaffolding (architecture + CI/TDD first)

## Why
Marketing wants reliable, daily analytics from **Wistia** (media- and visitor-level) across Facebook/YouTube to understand engagement and improve campaigns.

## Goals
- Automate **daily ingestion** from the Wistia Stats API (media + visitors).
- Store raw → curated → mart data in an **AWS-native** lakehouse.
- Support **incremental loads** (pagination + watermarks) and safe re-runs.
- Keep the system **testable, observable, and deployable** (CI/CD).
- Run in “production mode” for **7 consecutive days**.

## Scope (high level)
- **Ingestion:** Python Lambdas calling Wistia (token in Secrets Manager).  
- **State & retries:** Step Functions (Standard) with a Map state and capped concurrency.  
- **Watermarks:** DynamoDB (atomic “set-if-greater”) + 2-day transform grace window.  
- **Storage:** S3 Bronze (JSONL) → Silver/Gold in **Iceberg** (Glue Catalog, Athena).  
- **Transforms:** PySpark in **AWS Glue Job** (bronze→silver dedupe/upsert, silver→gold rollups).  
- **Observability:** CloudWatch metrics/alarms; failures → SNS/Slack.  
- **CI/CD:** GitHub Actions (OIDC to AWS), unit tests + nightly contract test.

## Architecture at a glance
- **EventBridge (cron daily)** → **Step Functions**  
  → **Lambda ingestion (paginated, incremental)** → **S3 Bronze**  
  → **DynamoDB watermark update** → **Glue Job (PySpark)**  
  → **Iceberg Silver/Gold** → **Glue Catalog + Athena** → (optional) Streamlit

## Data model (initial)
- `dim_media(media_id, title, url, channel, created_at)` — Type-1.
- `dim_visitor(visitor_id, ip_address, country)` — Type-1.
- `fact_media_engagement(media_id, visitor_id, date, play_count, play_rate, total_watch_time, watched_percent)` — grain: `(media_id, visitor_id, date)`.

## Process / Way of working
1. **Design-first:** finalize one-pager + diagram; define tunables (`per_page`, `max_concurrency`, `lookback_days=2`).  
2. **TDD-first:** start with unit tests for paginator/backoff/watermark; PySpark dedupe tests (small DataFrames).  
3. **CI-first:** Actions run lint + unit tests on every push; nightly contract test hits Wistia once via OIDC-assumed role.  
4. **Thin entry points:** Lambda/Glue are thin wrappers; logic lives in testable modules.  
5. **Idempotency:** Bronze immutable with `run_id` + `updated_at`; Silver MERGE (latest wins).

## Milestones (light)
- **Week 1:** API exploration, auth, paginator & watermark tests green.  
- **Week 2:** Bronze ingestion via Step Functions; basic metrics/alerts.  
- **Week 3:** PySpark bronze→silver (dedupe/upsert) + Iceberg tables.  
- **Week 4:** Silver→gold rollups; Athena checks.  
- **Week 5–6:** 7-day run; polish docs/diagram; (optional) Streamlit demo.

## Repo layout (planned)

- ingestion/ # Python client + Lambda handlers + tests
- transforms/ # PySpark jobs (Glue) + tests
- infra/ # SAM/CDK templates for Lambda, Step Fn, EventBridge, Glue, IAM
- .github/workflows # CI (lint/tests) + deploy (OIDC)
- docs/ # diagram, one-pager, runbook