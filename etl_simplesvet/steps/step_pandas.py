from etl_simplesvet.hooks.hook import HookPandas
from etl_simplesvet.ingesters.ingester import IngesterPandas
from etl_simplesvet.persisters.persister import PersisterPandas
from etl_simplesvet.transformers.transformer import TransformerPandas
from etl_simplesvet.step import Step

class StepPandas(Step):
    def run(self):
        input_file_name = "datasets/Vendas.csv"
        output_file_name = "datasets/Vendas_out.csv"
        hook = HookPandas()
        ingester = IngesterPandas(hook, input_file_name)
        persister = PersisterPandas(hook, output_file_name)
        transformer = TransformerPandas()

        df = ingester.ingest()
        print(df, df.columns, len(df))

        df = transformer.set_df(df).transform()
        print(df, df.columns, len(df))
        persister.persist(df)
        return True

