import pandas as pd
from hook import HookPandas
from ingester import IngesterPandas
from persister import PersisterPandas

def main():
	input_file_name = "datasets/Vendas.csv"
	output_file_name = "datasets/Vendas_out.csv"
	hook = HookPandas()
	ingester = IngesterPandas(hook, input_file_name)
	persister = PersisterPandas(hook, output_file_name)

	df = ingester.ingest()
	print(df, df.columns, len(df))

	df = df.assign(new_column="new value")
	print(df, df.columns, len(df))
	persister.persist(df)

if __name__ == "__main__":
	main()
