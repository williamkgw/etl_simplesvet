import pandas as pd
from hook import HookPandas
from ingester import IngesterPandas

def main():
	file_name = "datasets/Vendas.csv"
	hook = HookPandas()
	ingester = IngesterPandas(hook, file_name)

	df = ingester.ingest()
	print(df, df.columns, len(df))

if __name__ == "__main__":
	main()
