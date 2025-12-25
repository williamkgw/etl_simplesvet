from etl_simplesvet.ingesters.ingester_pandas_xlsx import IngesterPandasXLSX


class IngesterPandasMappingClients(IngesterPandasXLSX):

    def __init__(self, file_name):
        super().__init__(file_name)
        self._file_name=file_name

        MAPPING_COLUMNS = {
            "Origem": str,
            "Grupo": str
        }

        mapping_columns_keys = list(MAPPING_COLUMNS.keys())

        self._options = {
            "usecols": mapping_columns_keys,
            "dtype": MAPPING_COLUMNS
        }

    def _rename_columns(self, df):
        RENAME_MAP = {
            "Origem": "TX_ORGM",
            "Grupo": "TX_GRP"
        }

        return df.rename(columns = RENAME_MAP)

    def _treat_frame(self, df):
        df = df.copy()

        df = df.set_index("TX_ORGM")
        df = df.dropna(how = 'all', axis = "rows")
        df.index = df.index.str.lower()
        df = df[~df.index.duplicated(keep='last')]

        return df

    def ingest(self):
        df = super() \
            .pass_options(**self._options) \
            ._read() \
            .pipe(self._rename_columns) \
            .pipe(self._treat_frame)

        return df
