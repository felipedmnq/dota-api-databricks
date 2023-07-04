# Databricks notebook source
# MAGIC %md
# MAGIC ## Utils

# COMMAND ----------

# MAGIC %run "/Users/felipe.vasconcelos@artefact.com/game_lake_house/utilities/utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import json
from functools import lru_cache
from pyspark.sql.types import StructField, StructType, LongType
import pandas as pd
from tqdm import tqdm
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configs

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

Configs = MatchDetailConfigs()
session = HTTPRequester().create_session()
Extractor = Extractor(session)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Development

# COMMAND ----------

class Ingestor:
    def __init__(self,  url: str, table_name: str, dota_raw_path: str) -> None:
        self.url = url
        self.table_name = table_name
        self.dota_raw_path = dota_raw_path
        self.table_path = f"{self.dota_raw_path}/{self.table_name}" 

    # @lru_cache
    # def _get_data(self, match_id) -> list[dict]:

    #     url = f"{self.url}/{match_id}"
    #     response = self.session.get(url)
    #     return response.json()   
    
    def _save_data(self, data: dict) -> None:
        match_id = data["match_id"]
        path_to_save = f"/dbfs{self.table_path}_raw/{match_id}.json"

        json.dump(data, open(path_to_save, "w"))

        data = {"match_id": [match_id]}
        data_df = pd.DataFrame(data)
        spark_df = (spark.createDataFrame(data_df)
                        .write
                        .format("delta")
                        .mode("append")
                        .save(self.table_path)
                    )
        
    def _get_match_ids(self):
        (spark.read
            .format("delta")
            .load(f"{self.dota_raw_path}/match_history")
            .createOrReplaceTempView("match_history")    
        )

        (spark.read
            .format("delta")
            .load(self.table_path)
            .createOrReplaceTempView("match_details")     
        )

        query = """
        SELECT DISTINCT hist.match_id AS match_id
        FROM match_history AS hist
        LEFT JOIN match_details AS det
        ON hist.match_id = det.match_id
        WHERE det.match_id IS NULL
        """

        match_ids = spark.sql(query).toPandas()["match_id"].tolist()
        return match_ids
    
    def _optimize_delta(self) -> None:
        delta_table = DeltaTable.forPath(spark, self.table_path)
        delta_table.optimize().executeCompaction()
        delta_table.vacuum()
    
    def execute_job(self) -> None:
        match_ids = self._get_match_ids()
        for match_id in tqdm(match_ids[:10]): # limiting job
            url = f"{self.url}/{match_id}"
            data = self._get_data(url)
            if "match_id" in data:
                self._save_data(data)

        self._optimize_delta()

        

# COMMAND ----------

ingestor.execute_job()
