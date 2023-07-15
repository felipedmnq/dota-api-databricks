# Databricks notebook source
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from dataclasses import dataclass
import json
from pyspark.sql.types import StructType
import time

# COMMAND ----------

# import utils
%run "/Workspace/Repos/felipe.vasconcelos@artefact.com/dota-api-databricks/game_lake_house/utilities/utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configs

# COMMAND ----------

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
    TABLE_NAME: str = "match_details"
    SCHEMA_PATH: str = "/Workspace/Repos/felipe.vasconcelos@artefact.com/dota-api-databricks/game_lake_house/bronze/schemas/match_details_schema.json"
    # TABLE_PATH: str = f"{RAW_LAKE_PATH}/{TABLE_NAME}"

Configs = BronzeMatchDetailConfigs()

# COMMAND ----------

# dbutils.fs.mkdirs(Configs.CHECKPOINT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schemas

# COMMAND ----------

# json_schema = spark.read.format("delta").load(f"{Configs.ORIGIN_PATH}match_history").schema.json()
# json_schema

# COMMAND ----------

# converted_json_schema = json.loads(json_schema)
# struct_schema = StructType.fromJson(converted_json_schema)
# struct_schema

# COMMAND ----------

def get_schema_from_json(path: str) -> StructType:
    with open(path, "r") as json_schema:
        schema_json = json.load(json_schema)
        return StructType.fromJson(schema_json)
    
match_schema = get_schema_from_json(Configs.SCHEMA_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Develop

# COMMAND ----------

df_stream = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.maxFilesPerTrigger", 1000)
                .schema(match_schema)
                .load(Configs.RAW_DETAILS_PATH)
            )

# COMMAND ----------

stream = (df_stream.writeStream
        .format("delta")
        .option("checkpointLocation", Configs.CHECKPOINT)
        .start(Configs.TARGET_PATH)
        

# COMMAND ----------

time.sleep(60*5)
stream.processAllAvailable()
stream.stop()

# COMMAND ----------

df = spark.read.format("delta").load(Configs.TARGET_PATH)
df.display()

# COMMAND ----------


