# Databricks notebook source
import json
import sys
sys.path.insert(0, "/Workspace/Repos/felipe.vasconcelos@artefact.com/dota-api-databricks/game_lake_house/utils")
import utils
from datetime import datetime, timedelta
from delta.tables import DeltaTable
from pyspark.sql import DataFrame

# COMMAND ----------

def import_query(path: str):
    with open(path, "r") as file_:
        return file_.read()
    
def date_range(date_start, date_stop):
    start = datetime.strptime(date_start, "%Y-%m-%d")
    stop = datetime.strptime(date_stop, "%Y-%m-%d")

    dates = []
    while start <= stop:
        dates.append(start.strftime("%Y-%m-%d"))
        start += timedelta(days=1)

    return dates

date_range("2023-01-01", "2023-02-01")

# COMMAND ----------

table = "fs_dota_player_matches"
database = "silver_lakehouse"
db_table = f"{database}.{table}"
start_date = "2023-07-31"# datetime.now().date()
stop_date = "2023-07-01" # start_date - timedelta(days=30)
dates = utils.date_range(start_date, stop_date)
query = utils.import_query(f"{table}.sql")
# dates = date_range(start_date, stop_date)
# query = import_query(f"{table}.sql")

# COMMAND ----------

# def first_load(df: DataFrame, db_table: str, partition: str) -> None:
#     (df.write.format("delta")
#         .mode("overwrite")
#         .partitionBy(partition)
#         .saveAsTable(db_table))
    
# def upsert(df: DataFrame, delta_table: DeltaTable, id_columns) -> None:
#     join_cond = " and ".join([f"delta.{id_} = df.{id_}" for id_ in id_columns])

#     (delta_table.alias("delta")
#         .merge(df.alias("df"), join_cond)
#         .whenMatchedUpdateAll()
#         .whenNotMatchedInsertAll()
#         .execute())


# COMMAND ----------

df = spark.sql(query)

if not utils.table_exists(database, table, spark):
    dt = dates.pop(0)
    query_exec = quey.format(date=dt)
    df = spark.sql(query_exec)
    utils.first_load(df, db_table, "dt_ref")

delta_table = DeltaTable.forName(spark, db_table)

for dt in dates:
    query_exec = quey.format(date=dt)
    df = spark.sql(query_exec)
    utils.upsert(df, delta_table, ["dt_ref", "account_id"])
    
delta_table.vaccum()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM silver_lakehouse.fs_dota_player_matches

# COMMAND ----------


