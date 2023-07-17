-- Databricks notebook source
-- DBTITLE 1,Full Load dota_match_players table
-- MAGIC %sql
-- MAGIC DROP TABLE IF EXISTS bronze_lakehouse.dota_match_players;
-- MAGIC
-- MAGIC CREATE TABLE bronze_lakehouse.dota_match_players
-- MAGIC
-- MAGIC WITH tb_players AS (
-- MAGIC   SELECT explode(players) AS player FROM bronze_lakehouse.dota_match_details
-- MAGIC )
-- MAGIC SELECT 
-- MAGIC   player.*,
-- MAGIC   from_unixtime(player.start_time, "yyyy-MM-dd HH:mm:ss") AS td_match
-- MAGIC FROM tb_players;

-- COMMAND ----------


