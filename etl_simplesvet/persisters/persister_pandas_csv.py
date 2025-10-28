import pandas as pd

from etl_simplesvet.persister import Persister

class PersisterPandasCSV(Persister):

    def __init__(self, file_name):
        self.__writer_kwargs = {
        "path_or_buf": file_name,
        "decimal": ",",
        "sep": ";",
        "encoding": "latin1"
        }

    def pass_options(self, **kwargs):
        self.__writer_kwargs.update(kwargs)
        return self

    def persist(self, df):
        df.to_csv(**self.__writer_kwargs)

