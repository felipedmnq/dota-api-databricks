# Databricks notebook source
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from dataclasses import dataclass, field

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configs

# COMMAND ----------

@dataclass
class HistoryMatchesConfigs:
    OPENDOTA_URL: str = "https://api.opendota.com/api/proMatches"
    LAKE_PATH: str = "/mnt/datalake/game-lake-house"
    TABLE_NAME: str = "match_history"
    RAW_TABLE_PATH: str = f"{LAKE_PATH}/raw/dota/{TABLE_NAME}"
    INGESTION_MODE: dict = field(default_factory=lambda: {"histry": "histry", "new": "new"})

@dataclass
class MatchDetailConfigs:
    OPENDOTA_URL: str = "https://api.opendota.com/api/matches"
    RAW_LAKE_PATH: str = "/mnt/datalake/game-lake-house/raw/dota"
    TABLE_NAME: str = "match_details"
    TABLE_PATH: str = f"{RAW_LAKE_PATH}/{TABLE_NAME}"

# COMMAND ----------


