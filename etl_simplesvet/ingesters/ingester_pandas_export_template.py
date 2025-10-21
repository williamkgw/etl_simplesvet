from pandas import NA

from etl_simplesvet.ingesters.ingester_pandas_csv import IngesterPandasCSV

def get_necessary_cols():
    is_necessary_columns = {
        "ID do Item":True,
        "Mês":True,
        "Ano":True,
        "Medição":True,
        "Fx Verde Inf/Previsto":True,
        "Fx Verde Sup":True,
        "Fx Vermelha Inf":True,
        "Fx Vermelha Sup":True,
        "Fx Cliente Inf":True,
        "Fx Cliente Sup":True,
        "Item":True,
        "Indicador":True,
        "Usuário":True,
        "Tipo":True,
        "Auxiliar":True,
        "Totalizado":True,
        "Medido":True,
        "Calendário":True
    }

    necessary_cols = tuple(key for key, value in is_necessary_columns.items() if value)
    return necessary_cols

class IngesterPandasExportTemplate(IngesterPandasCSV):

    def __init__(self, hook, file_name):
        self._hook=hook
        self._file_name=file_name


    def _configure_hook(self):
        necessary_columns = get_necessary_cols()

        return self._hook \
                .reader \
                .option("io", self._file_name) \
                .option("index_col", necessary_columns[0]) \
                .option("usecols", necessary_columns)
    def _treat_frame(self, df):
        df = df.copy()

        df["Medição"] = NA
        return df


    def ingest(self):
        df = self \
            ._configure_hook() \
            .xlsx() \
            .pipe(self._treat_frame)

        return df

