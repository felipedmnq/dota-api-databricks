-- Databricks notebook source
-- MAGIC %sql
-- MAGIC SELECT 
-- MAGIC   match_id, 
-- MAGIC   COUNT(*)
-- MAGIC FROM bronze_lakehouse.dota_match_players
-- MAGIC WHERE account_id IS NOT NULL AND
-- MAGIC   PLAYER_SLOT IS NOT NULL
-- MAGIC GROUP BY 1
-- MAGIC HAVING COUNT(*) != 10

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT 
-- MAGIC   COUNT(*),
-- MAGIC   COUNT(DISTINCT match_id)
-- MAGIC FROM bronze_lakehouse.dota_match_details

-- COMMAND ----------


