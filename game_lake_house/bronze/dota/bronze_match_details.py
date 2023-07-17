# Databricks notebook source
# DBTITLE 1,Imports
import sys

sys.path.insert(0, "../../utils/")

import utils

from dataclasses import dataclass
import json
from pyspark.sql.types import StructType
import time
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Configs
spark.conf.set("spark.databricks.delta.optmizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

@dataclass
class BronzeMatchDetailConfigs:
    OPENDOTA_URL: str = "https://api.opendota.com/api/matches"
    LAKEHOUSE_PATH: str = "/mnt/datalake/game-lake-house"
    BRONZE_PATH: str = f"{LAKEHOUSE_PATH}/bonze/dota"
    ORIGIN_PATH: str = f"{LAKEHOUSE_PATH}/raw/dota/"
    CHECKPOINT: str = f"{BRONZE_PATH}/match_details_checkpoint"
    RAW_DETAILS_PATH: str = f"{ORIGIN_PATH}match_details_raw"
    TARGET_PATH: str = f"{BRONZE_PATH}/matches"
    DATABASE: str = "bronze_lakehouse"
    TABLE_NAME: str = "dota_match_details"
    SCHEMA_PATH: str = "/Workspace/Repos/felipe.vasconcelos@artefact.com/dota-api-databricks/game_lake_house/bronze/schemas/match_details_schema.json"

    # TABLE_PATH: str = f"{RAW_LAKE_PATH}/{TABLE_NAME}"

Configs = BronzeMatchDetailConfigs()

# COMMAND ----------

# DBTITLE 1,Schema
# json_schema = spark.read.format("delta").load(f"{Configs.ORIGIN_PATH}match_history").schema.json()
# json_schema

# converted_json_schema = json.loads(json_schema)
# struct_schema = StructType.fromJson(converted_json_schema)
# struct_schema

# def get_schema_from_json(path: str) -> StructType:
#     with open(path, "r") as json_schema:
#         schema_json = json.load(json_schema)
#         return StructType.fromJson(schema_json)
    
match_details_schema = utils.get_schema_from_json(Configs.SCHEMA_PATH)

# COMMAND ----------

# DBTITLE 1,Full Load
if utils.table_exists(Configs.DATABASE, Configs.TABLE_NAME, spark):
    print(f"Table '{Configs.TABLE_NAME}' already exists.")
else:
    print(f"Creating table '{Configs.TABLE_NAME}' and starting first load...")
    df = spark.read.json(Configs.RAW_DETAILS_PATH, schema=match_details_schema)
    df.write.format("delta").mode("overwrite").saveAsTable(f"{Configs.DATABASE}.{Configs.TABLE_NAME}")
    print(f"First load to '{Configs.TABLE_NAME}' succeed.")

# COMMAND ----------

# DBTITLE 1,Read Stream
df_stream = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.maxFilesPerTrigger", 1000)
                .schema(match_schema)
                .load(Configs.RAW_DETAILS_PATH)
            )

# COMMAND ----------

# DBTITLE 1,Write Stream
def upsert_delta(batch_id: int, df: DataFrame, delta_table: str):
    (delta_table.alias("delta")
        .merge(df.alias("new"), "delta.match_id = new.match_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    
delta_table = DeltaTable.forPath(spark, Configs.TARGET_PATH)

stream = (df_stream.writeStream
        .format("delta")
        .foreachBatch(lambda df, batch_id: upsert_delta(batch_id, df, delta_table))
        .option("checkpointLocation", Configs.CHECKPOINT)
        .start()
    )
        

# COMMAND ----------

# DBTITLE 1,Stream control
time.sleep(60*2)
stream.processAllAvailable()
stream.stop()

# COMMAND ----------


