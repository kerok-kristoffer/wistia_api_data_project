-- Replace <BUCKET> and <PREFIX>
CREATE EXTERNAL TABLE IF NOT EXISTS analytics_silver_wistia_events (
  at timestamp,
  event_type string,
  position_s double,
  ip_hmac string,
  processed_at timestamp
)
PARTITIONED BY (dt string, media_id string)
STORED AS PARQUET
LOCATION 's3://<BUCKET>/<PREFIX>/wistia/events/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

MSCK REPAIR TABLE analytics_silver_wistia_events;
