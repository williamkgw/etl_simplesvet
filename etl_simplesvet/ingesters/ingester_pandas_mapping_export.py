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
            "index_col": mapping_columns_keys[0],
            "usecols": mapping_columns_keys,
            "dtype": MAPPING_COLUMNS
        }

    def ingest(self):
        df = super() \
            .pass_options(**self._options) \
            ._read()

        return df
