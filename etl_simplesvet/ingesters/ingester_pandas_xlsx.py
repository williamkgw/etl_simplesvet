import pandas as pd

from etl_simplesvet.ingester import Ingester

class IngesterPandasXLSX(Ingester):

    def __init__(self, file_name):
        self._file_name=file_name
        self._reader_kwargs = {
            "io": file_name
        }

    def pass_options(self, **kwargs):
        self._reader_kwargs.update(kwargs)
        return self

    def _read(self):
        return pd.read_excel(**self._reader_kwargs)

    def ingest(self):
        return self._read()
