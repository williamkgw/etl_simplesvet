import pandas as pd
import datetime

from etl_simplesvet.ingesters.ingester_pandas_csv import IngesterPandasCSV

class IngesterPandasSales(IngesterPandasCSV):

    def __init__(self, file_name, end_date):
        super().__init__(file_name)
        self._file_name=file_name
        self._end_date=end_date

    def _fill_missing_code(self, df):
        df = df.copy()

        df['Código'] = df['Código'].fillna(0)
        return df

    def _get_sales_last_36_months(self, df, end_date):
        max_datetime = pd.to_datetime(end_date) + pd.tseries.offsets.DateOffset(days=1)
        min_datetime = max_datetime - pd.tseries.offsets.DateOffset(years=3)

        df = df.copy()
        mask =  (df['Data e hora'] > min_datetime) \
                & (df['Data e hora'] < max_datetime)

        return df[mask]

    def _correct_datetime_column(self, df):
        df = df.copy()

        df['Data e hora'] = pd.to_datetime(df['Data e hora'], errors = 'coerce', format = '%d/%m/%Y %H:%M')
        df = df.dropna(subset = 'Data e hora')
        return df

    def _treat_frame(self, df):
        df = df.copy()

        return df \
                .pipe(self._correct_datetime_column) \
                .pipe(self._fill_missing_code) \
                .pipe(self._get_sales_last_36_months, end_date=self._end_date) \
                .astype({
                    'Produto/serviço': str,
                    'Quantidade': float,
                    'Bruto': float
                })

    def ingest(self):
        df = super() \
            .ingest() \
            .pipe(self._treat_frame)

        return df
