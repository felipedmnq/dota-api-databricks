# Databricks notebook source
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from functools import lru_cache
import requests
from requests import Session
from dataclasses import dataclass, field

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
    INGESTION_MODE: dict = field(default_factory=lambda: {"histry": "histry", "new": "new"})

@dataclass
class MatchDetailConfigs:
    OPENDOTA_URL: str = "https://api.opendota.com/api/matches"
    RAW_LAKE_PATH: str = "/mnt/datalake/game-lake-house/raw/dota"
    TABLE_NAME: str = "match_details"
    TABLE_PATH: str = f"{RAW_LAKE_PATH}/{TABLE_NAME}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utilities

# COMMAND ----------

class Extractor:

    def __init__(self, session: Session) -> None:
        self.__session = session

    @lru_cache
    def get_data(self, url: str, **params) -> list[dict]:

        response = self.__session.get(url, params=params)
        return response.json()
    

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



# COMMAND ----------


