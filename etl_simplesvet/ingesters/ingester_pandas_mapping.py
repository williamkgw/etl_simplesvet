from etl_simplesvet.ingesters.ingester_pandas_xlsx import IngesterPandasXLSX

class IngesterPandasMapping(IngesterPandasXLSX):

    def __init__(self, file_name):
        super().__init__(file_name)
        self._file_name=file_name

        MAPPING_COLUMNS = {
            "Produto/serviço": str,
            "Categoria": str,
            "Pilar": str,
            "Grupo": str
        }

        mapping_columns_keys = list(MAPPING_COLUMNS.keys())

        self._options = {
            "index_col": mapping_columns_keys[0],
            "usecols": mapping_columns_keys,
            "dtype": MAPPING_COLUMNS
        }

    def ingest(self):
        df = super() \
            .pass_options(**self._options) \
            ._read()

        return df
