from pandas import NA

from etl_simplesvet.ingesters.ingester_pandas_xlsx import IngesterPandasXLSX

class IngesterPandasExport(IngesterPandasXLSX):

    def __init__(self, file_name, end_date):
        super().__init__(file_name)
        self._file_name=file_name
        self._end_date = end_date

        necessary_columns = self.__get_necessary_cols()

        self._options = {
            "usecols": necessary_columns
        }

    def _rename_columns(self, df):
        RENAME_MAP = {
            "ID do Item": "CD_ID_ITEM",
            "Mês": "VL_MES",
            "Ano": "VL_ANO",
            "Medição": "VL_MED",
            "Fx Verde Inf/Previsto": "TX_FX_VERD_INF_PREV",
            "Fx Verde Sup": "TX_FX_VERD_SUP",
            "Fx Vermelha Inf": "TX_FX_VERM_INF",
            "Fx Vermelha Sup": "TX_FX_VERM_SUP",
            "Fx Cliente Inf": "TX_FX_CLNT_INF",
            "Fx Cliente Sup": "TX_FX_CLNT_SUP",
            "Item": "TX_ITEM",
            "Indicador": "TX_IND",
            "Usuário": "TX_USUA",
            "Tipo": "TX_TIPO",
            "Auxiliar": "TX_AUX",
            "Totalizado": "TX_TOT",
            "Medido": "TX_IN_MED",
            "Calendário": "TX_CLDR"
        }

        return df.rename(columns = RENAME_MAP)

    def _clean_data_from_export(self, df, end_date):
        COLS_TO_RESET = (
             "VL_MED",
             "TX_FX_VERD_INF_PREV",
             "TX_FX_VERD_SUP",
             "TX_FX_VERM_INF",
             "TX_FX_VERM_SUP",
             "TX_FX_CLNT_INF",
             "TX_FX_CLNT_SUP",
        )

        df.loc[:, COLS_TO_RESET] = NA
        df.loc[:, "VL_MES"] = end_date.month - 1
        df.loc[:, "VL_ANO"] = end_date.year
        return df

    def __get_necessary_cols(self):
        IS_NECESSARY_COLUMNS = {
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

        necessary_cols = tuple(key for key, value in IS_NECESSARY_COLUMNS.items() if value)
        return necessary_cols

    def _treat_frame(self, df):
        df = df.copy()

        return df \
                .pipe(self._clean_data_from_export, end_date = self._end_date) \
                .set_index("CD_ID_ITEM")

    def ingest(self):
        df = super() \
            .pass_options(**self._options) \
            ._read() \
            .pipe(self._rename_columns) \
            .pipe(self._treat_frame)

        return df

