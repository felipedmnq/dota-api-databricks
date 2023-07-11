# Databricks notebook source
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from dataclasses import dataclass
import json
from pyspark.sql.types import StructType

# COMMAND ----------

# import utils
%run "/Workspace/Repos/felipe.vasconcelos@artefact.com/dota-api-databricks/game_lake_house/utilities/utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configs

# COMMAND ----------

@dataclass
class BronzeMatchDetailConfigs:
    OPENDOTA_URL: str = "https://api.opendota.com/api/matches"
    ORIGIN_PATH: str = "/mnt/datalake/game-lake-house/raw/dota/"
    RAW_DETAILS_PATH: str = f"{ORIGIN_PATH}match_details_raw"
    TABLE_NAME: str = "match_details"
    SCHEMA_PATH: str = "/Workspace/Repos/felipe.vasconcelos@artefact.com/dota-api-databricks/game_lake_house/bronze/schemas/match_details_schema.json"
    # TABLE_PATH: str = f"{RAW_LAKE_PATH}/{TABLE_NAME}"

Configs = BronzeMatchDetailConfigs()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schemas

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

df_01 = spark.read.json(Configs.RAW_DETAILS_PATH, match_schema)

# COMMAND ----------

df_01.display()

# COMMAND ----------

df_01.count()

# COMMAND ----------


