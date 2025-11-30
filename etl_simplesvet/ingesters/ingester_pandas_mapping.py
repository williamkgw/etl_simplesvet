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

    def _treat_mapping_sales(self, mapping_sales_df):
        # removing empty rows
        missing_mapping_sales_df = mapping_sales_df[mapping_sales_df.isna().all(axis=1)]
        mapping_sales_df = mapping_sales_df.dropna(how = 'all', axis = 0)

        # configuring the dataframes to catch case sensitive
        mapping_sales_df.index = mapping_sales_df.index.str.lower()

        # removing duplicated index
        mapping_sales_duplicated_df = mapping_sales_df[mapping_sales_df.index.duplicated(keep = False)]
        mapping_sales_df = mapping_sales_df[~mapping_sales_df.index.duplicated(keep='last')]

        return mapping_sales_df

    def ingest(self):
        df = super() \
            .pass_options(**self._options) \
            ._read() \
            .pipe(self._treat_mapping_sales)

        return df
