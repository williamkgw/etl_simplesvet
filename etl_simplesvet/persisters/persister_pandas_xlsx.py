
import pandas as pd

from etl_simplesvet.persister import Persister

class PersisterPandasXLSX(Persister):
    def __init__(self, file_name):
        self.__writer_kwargs = {
            "excel_writer": file_name
        }

    def pass_options(self, **kwargs):
        self.__writer_kwargs.update(kwargs)
        return self

    def persist(self, df):
        df.to_excel(**self.__writer_kwargs)
