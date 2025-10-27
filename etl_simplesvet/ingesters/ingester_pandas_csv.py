import pandas as pd

from etl_simplesvet.ingester import Ingester

class IngesterPandasCSV(Ingester):

    def __init__(self, file_name):
        self._reader_kwargs = {
            "filepath_or_buffer": file_name,
            "thousands": ".",
            "decimal": ",",
            "sep": ";",
            "encoding": "latin1"
        }

    def pass_options(self, **kwargs):
        self._reader_kwargs.update(kwargs)
        return self

    def _read(self):
        df = pd.read_csv(**self._reader_kwargs)
        return df

    def ingest(self):
        return self._read()
