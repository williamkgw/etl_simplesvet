from etl_simplesvet.ingesters.ingester_pandas_xlsx import IngesterPandasXLSX

class IngesterPandasMappingExport(IngesterPandasXLSX):

    def __init__(self, file_name):
        super().__init__(file_name)
        self._file_name=file_name

        MAPPING_COLUMNS = {
            "ID do Item": int,
            "Mês": str,
            "Ano": str,
            "Item": str,
            "Categoria": str,
            "Pilar": str,
            "Grupo": str,
            "Op": str,
            "Op_execao": str,
            "Multiplicador": int
        }

        mapping_columns_keys = list(MAPPING_COLUMNS.keys())

        self._options = {
            "usecols": mapping_columns_keys,
            "dtype": MAPPING_COLUMNS
        }

    def _rename_columns(self, df):
        RENAME_MAP = {
            "ID do Item": "CD_ID_ITEM",
            "Mês": "VL_MES",
            "Ano": "VL_ANO",
            "Item": "TX_ITEM",
            "Categoria": "TX_CAT",
            "Pilar": "TX_PIL",
            "Grupo": "TX_GRP",
            "Op": "TX_OP",
            "Op_execao": "TX_OP_EXCE",
            "Multiplicador": "VL_MLTP"
        }
        return df.rename(columns = RENAME_MAP)

    def _treat_frame(self, df):
        df = df.copy()

        return df \
                .drop(["VL_MES", "VL_ANO", "TX_ITEM"], axis = "columns") \
                .set_index("CD_ID_ITEM")

    def ingest(self):
        df = super() \
            .pass_options(**self._options) \
            ._read() \
            .pipe(self._rename_columns) \
            .pipe(self._treat_frame)

        return df
