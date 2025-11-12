CREATE OR REPLACE VIEW wistia_analytics.vw_media_perf AS
SELECT
  f.dt,
  f.media_id,
  d.title,
  d.project_name,
  f.unique_visitors,
  f.loads,
  f.plays,
  f.play_rate
FROM wistia_analytics.gold_fact_media_daily f
LEFT JOIN wistia_analytics.gold_dim_media d
  ON d.media_id = f.media_id;
