import pandas as pd
import sqlite3

from etl_simplesvet.ingester import Ingester

class IngesterPandasSQL(Ingester):

    def __init__(self, query):
        conn = sqlite3.connect("datasets/tablespace/warehouse.db")

        self._reader_kwargs = {
            "sql": query,
            "con": conn,
        }

    def pass_options(self, **kwargs):
        self._reader_kwargs.update(kwargs)
        return self

    def _read(self):
        return pd.read_sql_query(**self._reader_kwargs)

    def ingest(self):
        return self._read()
