CREATE EXTERNAL TABLE IF NOT EXISTS wistia_analytics.silver_media_history (
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
PARTITIONED BY (
  dt            date,
  media_id      string
)
STORED AS PARQUET
LOCATION 's3://kerok-wistia-silver/wistia/media_history/';