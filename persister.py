import pandas as pd

class PersisterPandas:

	def __init__(self, hook, file_name):
		self._hook=hook.connect()
		self._file_name=file_name

	def persist(self, df):
		self._hook \
			.writer \
			.option("path_or_buf", self._file_name) \
			.option("decimal", ",") \
			.option("sep", ";") \
			.option("encoding", "latin1") \
			.csv(df)
