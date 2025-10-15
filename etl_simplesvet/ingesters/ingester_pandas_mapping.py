from etl_simplesvet.ingester import Ingester

class IngesterPandasMapping(Ingester):

    def __init__(self, hook, file_name):
        self._hook=hook.connect()
        self._file_name=file_name

    def _configure_hook(self):
        MAPPING_COLUMNS = {
            "Produto/serviço": str,
            "Categoria": str,
            "Pilar": str,
            "Grupo": str
        }

        return self._hook \
                .reader \
                .option("io", self._file_name) \
                .option("index_col", "Produto/serviço") \
                .option("usecols", MAPPING_COLUMNS.keys()) \
                .option("dtype", MAPPING_COLUMNS)

    def ingest(self):
        df = self \
            ._configure_hook() \
            .xlsx()


        return df
