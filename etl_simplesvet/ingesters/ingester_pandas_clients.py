import pandas as pd

from etl_simplesvet.ingesters.ingester_pandas_csv import IngesterPandasCSV


class IngesterPandasClients(IngesterPandasCSV):

    def __init__(self, hook, file_name, end_date):
        self._hook=hook.connect()
        self._file_name=file_name
        self._end_date=end_date

    def _treat_frame(self, df):
        df = df.copy()

        df['Origem'] = df['Origem'].fillna('_outros')
        df['Inclusão'] = pd.to_datetime(df['Inclusão'], dayfirst = True, errors = 'coerce')
        df['Inclusão'] = df['Inclusão'].fillna('01/01/1900')
        mask = df['Inclusão'] <= pd.to_datetime(self._end_date)
        return df[mask]

    def ingest(self):
        df = super() \
            ._configure_hook() \
            .option("dayfirst", True) \
            .option("parse_dates", ["Inclusão"]) \
            .csv() \
            .pipe(self._treat_frame)

        return df
