from etl_simplesvet.ingester import Ingester

class IngesterPandasMappingExport(Ingester):

    def __init__(self, hook, file_name):
        self._hook=hook.connect()
        self._file_name=file_name

    def _configure_hook(self):
        MAPPING_COLUMNS = {
            "ID do Item": str,
            "Mês": str,
            "Ano": str,
            "Item": str,
            "Categoria": str,
            "Pilar": str,
            "Grupo": str,
            "Op": str,
            "Op_execao": str,
            "Multiplicador": str
        }

        mapping_columns_keys = list(MAPPING_COLUMNS.keys())

        return self._hook \
                .reader \
                .option("io", self._file_name) \
                .option("index_col", mapping_columns_keys[0]) \
                .option("usecols", mapping_columns_keys) \
                .option("dtype", MAPPING_COLUMNS)

    def ingest(self):
        df =  self \
            ._configure_hook() \
            .xlsx()

        return df



