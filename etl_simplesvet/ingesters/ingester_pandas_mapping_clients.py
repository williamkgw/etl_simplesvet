from etl_simplesvet.ingester import Ingester

class IngesterPandasMappingClients(Ingester):

    def __init__(self, hook, file_name):
        self._hook=hook.connect()
        self._file_name=file_name

    def _configure_hook(self):
        MAPPING_COLUMNS = {
            "Origem": str,
            "Grupo": str
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
