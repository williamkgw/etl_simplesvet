import pandas as pd
from connector import HookPandas

def main():

	data = {
	    "id": [1, 2, 3, 4, 5],
	    "nome": ["Alice", "Bruno", "Carla", "Diego", "Eva"],
	    "idade": [25, 30, 22, 35, 28],
	    "cidade": ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba", "Recife"],
	    "score": [85.5, 90.2, 76.8, 88.0, 92.3]
	}

	conn = HookPandas()
	conn.connect()

	df = pd.DataFrame(data)
	conn.writer \
 	.option("path_or_buf", "hihi.csv") \
 	.option("sep", ";") \
 	.option("header", True) \
	.csv(df)

if __name__ == "__main__":
	main()
