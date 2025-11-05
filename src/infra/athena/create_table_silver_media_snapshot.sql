CREATE EXTERNAL TABLE IF NOT EXISTS wistia_analytics.silver_media_snapshot (
  media_id       string,
  name           string,
  duration_s     double,
  created_at     timestamp,
  updated_at     timestamp,
  status         string,
  archived       boolean,
  section        string,
  project_id     bigint,
  project_name   string,
  thumbnail_url  string,
  progress       double,
  ingested_at    timestamp
)
STORED AS PARQUET
LOCATION 's3://kerok-wistia-silver/wistia/media_snapshot/';