from pandas import NA

from etl_simplesvet.ingesters.ingester_pandas_xlsx import IngesterPandasXLSX

class IngesterPandasExportTemplate(IngesterPandasXLSX):

    def __init__(self, file_name):
        super().__init__(file_name)
        self._file_name=file_name

        necessary_columns = self.__get_necessary_cols()

        self._options = {
            "index_col": necessary_columns[0],
            "usecols": necessary_columns
        }

    def __get_necessary_cols(self):
        is_necessary_columns = {
            "ID do Item": True,
            "Mês": True,
            "Ano": True,
            "Medição": True,
            "Fx Verde Inf/Previsto": True,
            "Fx Verde Sup": True,
            "Fx Vermelha Inf": True,
            "Fx Vermelha Sup": True,
            "Fx Cliente Inf": True,
            "Fx Cliente Sup": True,
            "Item": True,
            "Indicador": True,
            "Usuário": True,
            "Tipo": True,
            "Auxiliar": True,
            "Totalizado": True,
            "Medido": True,
            "Calendário": True
        }

        necessary_cols = tuple(key for key, value in is_necessary_columns.items() if value)
        return necessary_cols

    def _treat_frame(self, df):
        df = df.copy()

        df["Medição"] = NA
        return df


    def ingest(self):
        df = super() \
            .pass_options(**self._options) \
            ._read() \
            .pipe(self._treat_frame)

        return df

