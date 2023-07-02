# Databricks notebook source
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from functools import lru_cache
import requests
from requests import Session

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


