# Databricks notebook source
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import json
from requests import Session
from functools import lru_cache
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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


OPENDOTA_URL = "https://api.opendota.com/api/matches"
RAW_LAKE_PATH = "/mnt/datalake/game-lake-house/raw/dota"
TABLE_NAME = "match_details"
# TABLE_PATH = f"{RAW_LAKE_PATH}/{TABLE_NAME}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Development

# COMMAND ----------

class HTTPRequester:

    def __init__(self) -> None:
        self.session = self.create_session()

    @classmethod
    def create_session(
        cls,
        retries: int = 3,
        backoff_factor: float = 0.3,
        status_forcelist: tuple[int] = tuple(range(400, 430)) + (500, 502, 503, 504),
    ) -> Session:

        session = Session()

        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            allowed_methods=("GET")
        )

        session_adapter = HTTPAdapter(max_retries=retry)

        session.mount("http://", session_adapter)
        session.mount("https://", session_adapter)

        return session

# COMMAND ----------

class Ingestor:
    def __init__(self, session: Session,  url: str, table_name: str, dota_raw_path: str) -> None:
        self.session = session
        self.url = url
        self.table_name = table_name
        self.dota_raw_path = dota_raw_path
        self.table_path = f"{self.dota_raw_path}/{self.table_name}" 

    @lru_cache
    def _get_data(self, match_id) -> list[dict]:

        url = f"{self.url}/{match_id}"
        response = self.session.get(url)
        return response.json()   
    
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
        for id_ in tqdm(match_ids[:10]): # limiting job
            data = self._get_data(id_)
            if "match_id" in data:
                self._save_data(data)

        self._optimize_delta()

        

# COMMAND ----------

session = HTTPRequester().create_session()

ingestor = Ingestor(session, OPENDOTA_URL, TABLE_NAME, RAW_LAKE_PATH)

# COMMAND ----------

ingestor.execute_job()

# COMMAND ----------

# match_ids = ingestor.get_match_ids()
len(match_ids)

# COMMAND ----------

df = spark.read.format("delta").load(ingestor.table_path)
df.display()

# COMMAND ----------


