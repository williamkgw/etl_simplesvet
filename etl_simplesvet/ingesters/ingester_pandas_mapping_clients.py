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
            "index_col": mapping_columns_keys[0],
            "usecols": mapping_columns_keys,
            "dtype": MAPPING_COLUMNS
        }

    def _rename_columns(self, df):
        RENAME_MAP = {
            "Origem": "TX_ORGM",
            "Grupo": "TX_GRP"
        }

        return df.rename(columns = RENAME_MAP)

    def _treat_frame(self, mapping_clients_df):
        mapping_clients_df = mapping_clients_df.dropna(how = 'all', axis = "rows")
        mapping_clients_df.index = mapping_clients_df.index.str.lower()
        mapping_clients_df = mapping_clients_df[~mapping_clients_df.index.duplicated(keep='last')]

        return mapping_clients_df

    def ingest(self):
        df = super() \
            .pass_options(**self._options) \
            ._read() \
            .pipe(self._treat_frame)

        return df
