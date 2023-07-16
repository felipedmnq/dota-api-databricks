# Databricks notebook source
# DBTITLE 1,Imports
import sys

sys.path.insert(0, "../../utils/")

import utils

from typing import Dict, Tuple
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from datetime import datetime
import argparse
from functools import lru_cache
from dataclasses import dataclass, field
from typing import Literal

# COMMAND ----------

# DBTITLE 1,Configs
@dataclass
class HistoryMatchesConfigs:
    OPENDOTA_URL: str = "https://api.opendota.com/api/proMatches"
    LAKE_PATH: str = "/mnt/datalake/game-lake-house"
    TABLE_NAME: str = "match_history"
    RAW_TABLE_PATH: str = f"{LAKE_PATH}/raw/dota/{TABLE_NAME}"
    INGESTION_MODE: Literal["history", "new"] = "new"

Configs = HistoryMatchesConfigs()

# COMMAND ----------

# DBTITLE 1,Get matches history
class Ingestor:
    def __init__(self, extractor, url: str, path_to_save: str) -> None:
        self.extractor = extractor
        self.url = url
        self.path_to_save = path_to_save

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
        data = self.extractor.get_data(self.url, **params)
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

session = utils.HTTPRequester().create_session()
extractor = utils.Extractor(session)
match_ingestor = Ingestor(extractor, Configs.OPENDOTA_URL, Configs.RAW_TABLE_PATH)
INGESTION_MODE = Configs.INGESTION_MODE


if INGESTION_MODE == "history":
    min_match_id = match_ingestor._get_current_min_max_match_id()[0]
    data = match_ingestor._get_and_save(less_than_match_id=min_match_id)
    match_ingestor.get_all_matches(data=data, hist=True)
elif INGESTION_MODE == "new":
    match_ingestor.get_all_matches()


# COMMAND ----------


