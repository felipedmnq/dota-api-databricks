__all__ = ["get_schema_from_json", "table_exists", "HTTPRequester", "Extractor", "import_query"]

import json
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession, DataFrame
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from functools import lru_cache
import requests
from requests import Session
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from delta.tables import DeltaTable


def get_schema_from_json(path: str) -> StructType:
    with open(path, "r") as json_schema:
        schema_json = json.load(json_schema)
        return StructType.fromJson(schema_json)
    
def import_query(path: str):
    with open(path, "r") as file_:
        return file_.read()

def table_exists(database: str, table_name: str, spark: SparkSession) -> bool:
    count = (spark.sql(f"SHOW TABLES IN {database}")
                .filter(f"tableName = '{table_name}'")
                .count()
            )
    return count == 1

def date_range(date_start, date_stop):
    start = datetime.strptime(date_start, "%Y-%m-%d")
    stop = datetime.strptime(date_stop, "%Y-%m-%d")

    dates = []
    while start <= stop:
        dates.append(start.strftime("%Y-%m-%d"))
        start += timedelta(days=1)

    return dates

def first_load(df: DataFrame, db_table: str, partition: str) -> None:
    (df.write.format("delta")
        .mode("overwrite")
        .partitionBy(partition)
        .saveAsTable(db_table))
    
def upsert(df: DataFrame, delta_table: DeltaTable, id_columns) -> None:
    join_cond = " and ".join([f"delta.{id_} = df.{id_}" for id_ in id_columns])

    (delta_table.alias("delta")
        .merge(df.alias("df"), join_cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

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
    

class Extractor:

    def __init__(self, session: Session) -> None:
        self.__session = session

    @lru_cache
    def get_data(self, url: str, **params) -> list[dict]:

        response = self.__session.get(url, params=params)
        return response.json()