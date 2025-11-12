CREATE EXTERNAL TABLE IF NOT EXISTS wistia_analytics.gold_dim_media (
  media_id         string,
  title            string,
  duration_s       double,
  created_at       timestamp,
  updated_at       timestamp,
  status           string,
  archived         boolean,
  section          string,
  project_id       bigint,
  project_name     string,
  thumbnail_url    string,
  progress         double,
  gold_ingested_at timestamp
)
STORED AS PARQUET
LOCATION 's3://kerok-wistia-gold/gold/wistia/dim_media/';