import pandas as pd

class IngesterPandas:

	def __init__(self, hook, file_name):
		self._hook=hook.connect()
		self._file_name=file_name

	def ingest(self):
		df = self._hook \
			.reader \
			.option("filepath_or_buffer", self._file_name) \
			.option("thousands", ".") \
			.option("decimal", ",") \
			.option("sep", ";") \
			.option("encoding", "latin1") \
			.csv()
		return df
