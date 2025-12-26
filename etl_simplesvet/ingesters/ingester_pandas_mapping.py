from etl_simplesvet.ingesters.ingester_pandas_xlsx import IngesterPandasXLSX

class IngesterPandasMapping(IngesterPandasXLSX):

    def __init__(self, file_name):
        super().__init__(file_name)
        self._file_name=file_name
        self._options=dict()

    def _rename_columns(self, df):
        RENAME_MAP = {
            "Produto/serviço": "TX_PRD_SRV",
            "Categoria": "TX_CAT",
            "Pilar": "TX_PIL",
            "Grupo": "TX_GRP"
        }

        return df.rename(columns = RENAME_MAP)

    def _treat_frame(self, df):
        MAPPING_COLUMNS = {
            "TX_PRD_SRV": str,
            "TX_CAT": str,
            "TX_PIL": str,
            "TX_GRP": str
        }
        mapping_columns_keys = list(MAPPING_COLUMNS.keys())

        df = df[mapping_columns_keys]
        df = df.astype(MAPPING_COLUMNS)
        index_col = mapping_columns_keys.pop(0)
        df = df.set_index(index_col)
        df = df.dropna(how = "all", axis = "rows")
        df.index = df.index.str.lower()
        df = df[~df.index.duplicated(keep="last")]

        return df

    def ingest(self):
        df = super() \
            .pass_options(**self._options) \
            ._read() \
            .pipe(self._rename_columns) \
            .pipe(self._treat_frame)

        return df
