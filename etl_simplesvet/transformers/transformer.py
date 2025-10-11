class TransformerPandas:

    def set_df(self, df):
        self._df = df
        return self

    def transform(self):
        df = self._df.copy()
        df = df.assign(new_column="new value")
        return df
