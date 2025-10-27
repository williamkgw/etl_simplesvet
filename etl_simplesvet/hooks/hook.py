import pandas as pd

from etl_simplesvet.hook import Hook

class HookPandasWriter:

    def __init__(self):
        self._options = dict()

        self.csv = lambda df: df.to_csv(**self._options)
        self.xlsx = lambda df: df.to_excel(**self._options)

    def option(self, option_key, option_value):
        self._options.update({
        option_key: option_value
        })
        return self

    def options(self, options_dict):
        self._options.update(options_dict)
        return self

class HookPandasReader:

    def __init__(self):
        self._options = dict()

        self.csv = lambda : pd.read_csv(**self._options)
        self.xlsx = lambda : pd.read_excel(**self._options)

    def option(self, option_key, option_value):
        self._options.update({
        option_key: option_value
        })
        return self

    def options(self, options_dict):
        self._options.update(options_dict)
        return self

class HookPandas(Hook):

    def connect(self):
        self.writer = HookPandasWriter()
        self.reader = HookPandasReader()
        return self
