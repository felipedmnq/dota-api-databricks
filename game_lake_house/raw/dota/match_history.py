# Databricks notebook source
# MAGIC %md
# MAGIC ## Utils

# COMMAND ----------

# MAGIC %run "/Users/felipe.vasconcelos@artefact.com/game_lake_house/utilities/utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from typing import Dict, Tuple
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from datetime import datetime
import argparse
from functools import lru_cache
from dataclasses import dataclass, field
from typing import Literal

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
    INGESTION_MODE: Leteral["history", "new"] = "new"

Configs = HistoryMatchesConfigs()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Development

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get pro matches history
# MAGIC - Get data from each dota pro match.

# COMMAND ----------

class Ingestor:
    def __init__(self, session: Session, url: str, path_to_save: str) -> None:
        self.__session = session
        self.url = url
        self.path_to_save = path_to_save

    @lru_cache
    def _get_data(self, **params) -> list[dict]:

        response = self.__session.get(self.url, params=params)
        return response.json()

    def _get_min_match_id(self, data: list[dict]) -> int:
        return min([item["match_id"] for item in data])

    def _save_data(self, df: DataFrame) -> None:
        (df.coalesce(1)
            .write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(self.path_to_save))
        
    def _get_and_save(self, **params) -> list[dict]:
        data = self._get_data(**params)
        df = spark.createDataFrame(data)
        df = self._augment_start_time(df)
        self._save_data(df)
        return data
    
    def _augment_start_time(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "td_start", F.from_unixtime(F.col("start_time"), format="yyyy-MM-dd HH:mm:ss")
        )

    def _get_current_min_max_match_id(self) -> int:
        df = spark.read.format("delta").load(Configs.RAW_TABLE_PATH)
        min_match_id = df.select(F.min(F.col("match_id"))).collect()[0][0]
        max_match_id = df.select(F.max(F.col("match_id"))).collect()[0][0]
        return min_match_id, max_match_id
        
    def get_all_matches(
        self,
        data: list[dict] = None, 
        limit: int = 30,
        hist: bool = False
    ) -> None:

        if not hist and data is None:
            print("getting new data...")
            data = self._get_and_save()
            min_match_id = self._get_min_match_id(data)
            current_max_match_id = self._get_current_min_max_match_id()[-1]
            print(min_match_id, current_max_match_id)
            while min_match_id > current_max_match_id:
                data = self._get_and_save(less_than_match_id=min_match_id)
                min_match_id = self._get_min_match_id(data)
                print(min_match_id)

        elif hist and data is not None:
            print("getting history data...")
            count = 0
            while count < 30:
                min_match_id = self._get_min_match_id(data)
                data = self._get_and_save(less_than_match_id=min_match_id)
                count += 1
                print(min_match_id)

# COMMAND ----------

session = HTTPRequester().create_session()
match_ingestor = Ingestor(session, Configs.OPENDOTA_URL, Configs.RAW_TABLE_PATH)
INGESTION_MODE = Configs.INGESTION_MODE["new"]


if INGESTION_MODE == "history":
    min_match_id = match_ingestor._get_current_min_max_match_id()[0]
    data = match_ingestor._get_and_save(less_than_match_id=min_match_id)
    match_ingestor.get_all_matches(data=data, hist=True)
elif INGESTION_MODE == "new":
    match_ingestor.get_all_matches()

