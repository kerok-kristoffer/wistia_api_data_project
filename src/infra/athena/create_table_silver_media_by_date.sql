-- Replace <BUCKET> and <PREFIX> (PREFIX = your silver root prefix, e.g., kerok-wistia)
CREATE EXTERNAL TABLE IF NOT EXISTS analytics_silver_wistia_media_by_date (
  day date,
  load_count bigint,
  play_count bigint,
  play_rate double,
  hours_watched double,
  engagement double,
  visitors bigint,
  processed_at timestamp
)
PARTITIONED BY (dt string, media_id string)
STORED AS PARQUET
LOCATION 's3://<BUCKET>/<PREFIX>/wistia/media_by_date/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

MSCK REPAIR TABLE analytics_silver_wistia_media_by_date;
