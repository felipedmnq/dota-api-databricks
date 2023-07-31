WITH tb_game_stats AS (
  SELECT
    account_id,
    COUNT(DISTINCT match_id) As qt_matches,
    MIN(datediff(now(), td_match)) AS qt_days_last_match,
    COUNT(DISTINCT match_id) / COUNT(DISTINCT month(td_match)) AS tq_match_month,
    COUNT(DISTINCT match_id) / 6 AS tq_match_month_6,
    AVG(win) AS avg_win_rate,
    AVG(kills_per_min) AS vl_avg_kill_min,
    AVG(assists) AS avg_assists,
    AVG(camps_stacked) AS vl_avg_camps_stacked,
    AVG(creeps_stacked) AS vl_avg_creeps_stacked,
    AVG(deaths) AS vl_avg_deaths,
    AVG(denies) AS vl_avg_denies,
    AVG(firstblood_claimed) AS vl_avg_firstblood_claime,
    AVG(gold_per_min) AS vl_avg_gold_per_min,
    AVG(kills) AS vl_avg_kills,
    AVG(rune_pickups) AS vl_avg_rune,
    AVG(xp_per_min) AS vl_avg_xp_per_min,
    AVG(kda) AS vl_avg_kda,
    AVG(neutral_kills) AS vl_avg_neutral_kills,
    AVG(tower_kills) AS vl_avg_tower_kills,
    AVG(observer_kills) AS vl_avg_observer_kills,
    AVG(sentry_kills) AS vl_avg_sentry_kills,
    SUM(CASE WHEN lane_role = 1 THEN 1 ELSE 0 END) / COUNT(DISTINCT match_id) AS pctg_lane_role_1,
    SUM(CASE WHEN lane_role = 2 THEN 1 ELSE 0 END) / COUNT(DISTINCT match_id) AS pctg_lane_role_2,
    SUM(CASE WHEN lane_role = 3 THEN 1 ELSE 0 END) / COUNT(DISTINCT match_id) AS pctg_lane_role_3,
    SUM(CASE WHEN lane_role = 4 THEN 1 ELSE 0 END) / COUNT(DISTINCT match_id) AS pctg_lane_role_4
  FROM bronze_lakehouse.dota_match_players
  WHERE td_match > add_months(now(), -6)
  AND td_match < now()
  GROUP BY account_id
),
tb_lifetime AS (
  SELECT 
    account_id,
    MAX(datediff('{date}', td_match)) AS tq_days_first_match
  FROM bronze_lakehouse.dota_match_players
  GROUP BY 1
)
SELECT
  DATE('{date}') AS dt_ref,
  t1.account_id,
  COALESCE(t1.qt_matches, 0) AS qt_matches,
  COALESCE(t1.qt_days_last_match, 0) AS qt_days_last_match,
  COALESCE(t2.tq_days_first_match, 0) AS tq_days_first_match,
  COALESCE(t1.tq_match_month, 0) AS tq_match_month,
  COALESCE(t1.tq_match_month_6, 0) AS tq_match_month_6,
  COALESCE(t1.avg_win_rate, 0) AS avg_win_rate,
  COALESCE(t1.vl_avg_kill_min, 0) AS vl_avg_kill_min,
  COALESCE(t1.avg_assists, 0) AS avg_assists,
  COALESCE(t1.vl_avg_camps_stacked, 0) AS vl_avg_camps_stacked,
  COALESCE(t1.vl_avg_creeps_stacked, 0) AS vl_avg_creeps_stacked,
  COALESCE(t1.vl_avg_deaths, 0) AS vl_avg_deaths,
  COALESCE(t1.vl_avg_denies, 0) AS vl_avg_denies,
  COALESCE(t1.vl_avg_firstblood_claime, 0) AS vl_avg_firstblood_claime,
  COALESCE(t1.vl_avg_gold_per_min, 0) AS vl_avg_gold_per_min,
  COALESCE(t1.vl_avg_kills, 0) AS vl_avg_kills,
  COALESCE(t1.vl_avg_rune, 0) AS vl_avg_rune,
  COALESCE(t1.vl_avg_xp_per_min, 0) AS vl_avg_xp_per_min,
  COALESCE(t1.vl_avg_kda, 0) AS vl_avg_kda,
  COALESCE(t1.vl_avg_neutral_kills, 0) AS vl_avg_neutral_kills,
  COALESCE(t1.vl_avg_tower_kills, 0) AS vl_avg_tower_kills,
  COALESCE(t1.vl_avg_observer_kills, 0) AS vl_avg_observer_kills,
  COALESCE(t1.vl_avg_sentry_kills, 0) AS vl_avg_sentry_kills,
  COALESCE(t1.pctg_lane_role_1, 0) AS pctg_lane_role_1,
  COALESCE(t1.pctg_lane_role_2, 0) AS pctg_lane_role_2,
  COALESCE(t1.pctg_lane_role_3, 0) AS pctg_lane_role_3,
  COALESCE(t1.pctg_lane_role_4, 0) AS pctg_lane_role_4
FROM tb_game_stats AS t1
LEFT JOIN tb_lifetime AS t2
ON t1.account_id = t2.account_id