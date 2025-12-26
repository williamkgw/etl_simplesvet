import pandas as pd
import datetime

from etl_simplesvet.ingesters.ingester_pandas_csv import IngesterPandasCSV

class IngesterPandasSales(IngesterPandasCSV):

    def __init__(self, file_name, end_date):
        super().__init__(file_name)
        self._file_name=file_name
        self._end_date=end_date

    def _rename_columns(self, df):
        RENAME_MAP = {
            "Data e hora": "TS_DT_HR_VND",
            "Venda": "CD_VND",
            "Status da venda": "IN_STS_VND",
            "Data baixa": "TS_DT_BXA_VND",
            "Forma pagamento": "TX_FRM_PGT",
            "Funcionário": "TX_NM_FUN",
            "Cliente": "TX_NM_CLI",
            "Código": "CD_CLI",
            "CPF": "CD_CPF",
            "Sexo": "IN_SEX_CLI",
            "CEP": "CD_CEP",
            "Endereço": "TX_END",
            "Número": "TX_NUM_END",
            "Bairro": "TX_BAI",
            "Email": "TX_EML",
            "Celular": "TX_TEL",
            "Animal": "TX_NM_ANM",
            "Espécie": "TX_ESP_ANM",
            "Sexo.1": "IN_SEX_ANM",
            "Raça": "TX_RACA_ANM",
            "Tipo do Item": "TX_TP_ITM",
            "Grupo": "TX_GRP_SMP_VET",
            "Produto/serviço": "TX_PRD_SRV",
            "Valor Unitário": "VL_UNT",
            "Quantidade": "VL_QTD",
            "Bruto": "VL_BRT",
            "Desconto": "VL_DSC",
            "Líquido": "VL_LIQ",
            "Observações": "TX_OBS"
        }

        return df.rename(columns = RENAME_MAP)

    def _fill_missing_code(self, df):
        df["CD_CLI"] = df["CD_CLI"].fillna(0)
        return df

    def _treat_product_service(self, df):
        df["TX_PRD_SRV"] = df["TX_PRD_SRV"].str.lower()
        return df

    def _get_sales_last_36_months(self, df, end_date):
        max_datetime = pd.to_datetime(end_date) + pd.tseries.offsets.DateOffset(days=1)
        min_datetime = max_datetime - pd.tseries.offsets.DateOffset(years=3)

        df = df.copy()
        mask =  (df["TS_DT_HR_VND"] > min_datetime) \
                & (df["TS_DT_HR_VND"] < max_datetime)

        return df[mask]

    def _correct_datetime_column(self, df):
        df["TS_DT_HR_VND"] = pd.to_datetime(df["TS_DT_HR_VND"], errors = "coerce", format = "%d/%m/%Y %H:%M")
        df = df.dropna(subset = "TS_DT_HR_VND")
        return df

    def _treat_frame(self, df):
        df = df.copy()

        return df \
                .pipe(self._rename_columns) \
                .pipe(self._correct_datetime_column) \
                .pipe(self._fill_missing_code) \
                .pipe(self._get_sales_last_36_months, end_date=self._end_date) \
                .pipe(self._treat_product_service) \
                .astype({
                    "TX_PRD_SRV": str,
                    "VL_QTD": float,
                    "VL_BRT": float
                })

    def ingest(self):
        df = super() \
            .ingest() \
            .pipe(self._treat_frame)

        return df
