CREATE EXTERNAL TABLE IF NOT EXISTS wistia_analytics.gold_fact_media_daily (
  media_id         string,
  unique_visitors  bigint,
  loads            bigint,
  plays            bigint,
  play_rate        double,
  gold_ingested_at timestamp
)
PARTITIONED BY (dt date)
STORED AS PARQUET
LOCATION 's3://kerok-wistia-gold/gold/wistia/fact_media_daily/';

-- fix partitions for Hive-style folders
MSCK REPAIR TABLE wistia_analytics.gold_fact_media_daily;