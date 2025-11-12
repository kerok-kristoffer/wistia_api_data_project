
-- Top videos by play rate (last 7 days)
SELECT
  d.title,
  d.project_name,
  f.media_id,
  CAST(AVG(f.play_rate) AS DOUBLE)       AS avg_play_rate,
  SUM(f.plays)                            AS total_plays,
  SUM(f.loads)                            AS total_loads,
  SUM(f.unique_visitors)                  AS total_unique_visitors
FROM wistia_analytics.gold_fact_media_daily f
LEFT JOIN wistia_analytics.gold_dim_media d
  ON d.media_id = f.media_id
WHERE f.dt BETWEEN date_add('day', -7, current_date) AND current_date
GROUP BY d.title, d.project_name, f.media_id
HAVING SUM(f.loads) > 0
ORDER BY avg_play_rate DESC
LIMIT 20;



-- Daily trend of plays & play rate
WITH win AS (
  SELECT DATE '2025-11-08' AS start_dt, DATE '2025-11-18' AS end_dt
)
SELECT
  f.dt,
  SUM(f.plays)                       AS plays,
  SUM(f.loads)                       AS loads,
  CASE WHEN SUM(f.loads) > 0
       THEN SUM(f.plays) / SUM(f.loads)
       ELSE 0.0 END                  AS play_rate
FROM wistia_analytics.gold_fact_media_daily f, win
WHERE f.dt BETWEEN win.start_dt AND win.end_dt
GROUP BY f.dt
ORDER BY f.dt;


-- Most viewed videos (last 7 days)
SELECT
  d.title,
  d.project_name,
  f.media_id,
  SUM(f.plays)            AS total_plays,
  SUM(f.unique_visitors)  AS total_unique_visitors
FROM wistia_analytics.gold_fact_media_daily f
LEFT JOIN wistia_analytics.gold_dim_media d
  ON d.media_id = f.media_id
WHERE f.dt BETWEEN date_add('day', -7, current_date) AND current_date
GROUP BY d.title, d.project_name, f.media_id
ORDER BY total_plays DESC
LIMIT 20;



-- Conversion-like view: visitors → plays (last 14 days)
SELECT
  d.title,
  f.media_id,
  SUM(f.unique_visitors)                             AS visitors,
  SUM(f.plays)                                       AS plays,
  CASE WHEN SUM(f.unique_visitors) > 0
       THEN CAST(SUM(f.plays) AS DOUBLE) / SUM(f.unique_visitors)
       ELSE 0.0 END                                  AS plays_per_visitor
FROM wistia_analytics.gold_fact_media_daily f
LEFT JOIN wistia_analytics.gold_dim_media d
  ON d.media_id = f.media_id
WHERE f.dt BETWEEN date_add('day', -14, current_date) AND current_date
GROUP BY d.title, f.media_id
ORDER BY plays_per_visitor DESC
LIMIT 20;


-- Data freshness checks
-- Latest partition date present
SELECT MAX(dt) AS latest_dt
FROM wistia_analytics.gold_fact_media_daily;

-- Rows written on a specific day
SELECT dt, COUNT(*) AS rows
FROM wistia_analytics.gold_fact_media_daily
WHERE dt = DATE '2025-11-08'
GROUP BY dt;
