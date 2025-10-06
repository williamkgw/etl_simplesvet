from hook import HookPandas
from ingester import IngesterPandas
from persister import PersisterPandas
from transformer import TransformerPandas

class StepOther:
    def run(self):
        print("Other Step to run")

class StepPandas:
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
