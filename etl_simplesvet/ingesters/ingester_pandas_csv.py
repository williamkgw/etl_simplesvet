import pandas as pd

from etl_simplesvet.ingester import Ingester

class IngesterPandasCSV(Ingester):

    def __init__(self, hook, file_name):
        self._hook=hook.connect()
        self._file_name=file_name

    def _configure_hook(self):
        return self._hook \
        .reader \
        .option("filepath_or_buffer", self._file_name) \
        .option("thousands", ".") \
        .option("decimal", ",") \
        .option("sep", ";") \
        .option("encoding", "latin1")

    def ingest(self):
        df = self \
            .__configure_hook() \
            .csv()

        return df
