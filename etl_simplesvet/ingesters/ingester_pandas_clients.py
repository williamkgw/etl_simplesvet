import pandas as pd

from etl_simplesvet.ingesters.ingester_pandas_csv import IngesterPandasCSV

class IngesterPandasClients(IngesterPandasCSV):

    def __init__(self, file_name, end_date):
        super().__init__(file_name)
        self._end_date=end_date
        self._options = {
            "dayfirst": True,
            "parse_dates": ["Inclusão"]
        }

    def _treat_frame(self, df, end_date):
        df = df.copy()
        df['Origem'] = df['Origem'].fillna('_outros').str.lower()
        df['Inclusão'] = pd.to_datetime(df['Inclusão'], dayfirst = True, errors = 'coerce')
        df['Inclusão'] = df['Inclusão'].fillna('01/01/1900')
        mask = df['Inclusão'] <= pd.to_datetime(end_date)
        return df[mask]

    def ingest(self):
        df = super() \
            .pass_options(**self._options) \
            ._read() \
            .pipe(self._treat_frame, end_date=self._end_date)

        return df
