CREATE EXTERNAL TABLE IF NOT EXISTS wistia_analytics.silver_visitors (
  visitor_key    string,
  load_count     bigint,
  play_count     bigint,
  name           string,
  email          string,
  ingested_at    timestamp
)
PARTITIONED BY (
  dt             date,
  media_id       string
)
STORED AS PARQUET
LOCATION 's3://kerok-wistia-silver/wistia/visitors/';