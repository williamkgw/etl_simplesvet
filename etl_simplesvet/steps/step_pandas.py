from etl_simplesvet.hooks.hook import HookPandas
from etl_simplesvet.ingesters.ingester_pandas_csv import IngesterPandasCSV
from etl_simplesvet.persisters.persister_pandas_csv import PersisterPandasCSV
from etl_simplesvet.transformers.transformer import TransformerPandas
from etl_simplesvet.step import Step

class StepPandas(Step):
    def run(self):
        input_file_name = "datasets/Vendas.csv"
        output_file_name = "datasets/Vendas_out.csv"
        ingester = IngesterPandasCSV(input_file_name)
        persister = PersisterPandasCSV(output_file_name)
        transformer = TransformerPandas()

        df = ingester.ingest()
        print(df, df.columns, len(df))

        df = transformer.set_df(df).transform()
        print(df, df.columns, len(df))
        persister.persist(df)
        return True

