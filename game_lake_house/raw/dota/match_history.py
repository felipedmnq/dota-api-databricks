# Databricks notebook source
# MAGIC %md
# MAGIC ## Utils

# COMMAND ----------

# MAGIC %run "/Users/felipe.vasconcelos@artefact.com/game_lake_house/utilities/utilities"

# COMMAND ----------

# MAGIC %run "/Users/felipe.vasconcelos@artefact.com/game_lake_house/utilities/configs"

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configs

# COMMAND ----------

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
        # self.date_stop = datetime.strptime(date_stop, "%Y-%m-%d")

    @lru_cache
    def _get_data(self, **params) -> list[dict]:

        response = self.__session.get(self.url, params=params)
        return response.json()

    def _get_min_match_id(self, data: list[dict]) -> int:
        return min([item["match_id"] for item in data])
    
    # def _get_current_min_max_match_id(self) -> int:
    #     df = spark.read.format("delta").load(RAW_PATH)
    #     min_max = df.select(F.min(F.col("match_id")), F.max(F.col("match_id"))).collect()
    #     return min_max[0][0], min_max[0][-1]

    def _save_data(self, df: DataFrame) -> None:
        (df.coalesce(1)
            .write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(self.path_to_save))
        
    # def _get_min_date(self, df: DataFrame) -> datetime.date:
    #     return df.select(F.min(F.to_date("dt_start"))).collect()[0][0]
        
    def _get_and_save(self, **params) -> list[dict]:
        data = self._get_data(**params)
        df = spark.createDataFrame(data)
        df = self._augment_start_time(df)
        # min_date = self._get_min_date(df)
        self._save_data(df)
        return data #, min_date
    
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
        # while len(data) == 100:

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


if INGESTION_MODE == "history":
    min_match_id = match_ingestor._get_current_min_max_match_id()[0]
    data = match_ingestor._get_and_save(less_than_match_id=min_match_id)
    match_ingestor.get_all_matches(data=data, hist=True)
elif INGESTION_MODE == "new":
    match_ingestor.get_all_matches()


# COMMAND ----------

data_test = match_ingestor._get_data()
min_, max_ = match_ingestor._get_current_min_max_match_id()
min_, max_

# COMMAND ----------

data_test

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests

# COMMAND ----------

df = spark.read.format("delta").load(RAW_PATH)
min_max = df.select(F.min(F.col("match_id")), F.max(F.col("match_id"))).collect()
min_max = min_max[0][0], min_max[0][-1]
print(min_max)

# COMMAND ----------

df.select(F.countDistinct(F.col("match_id"))).display()

# COMMAND ----------



# COMMAND ----------


