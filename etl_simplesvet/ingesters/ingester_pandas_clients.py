import pandas as pd

from etl_simplesvet.ingesters.ingester_pandas_csv import IngesterPandasCSV

class IngesterPandasClients(IngesterPandasCSV):

    def __init__(self, file_name, end_date):
        super().__init__(file_name)
        self._end_date=end_date
        self._options = {
            "dayfirst": True,
            "parse_dates": ["Inclusão"]
        }

    def _rename_columns(self, df):
        RENAME_MAP = {
            "Ficha": "CD_FCH",
            "Nome": "TX_NM",
            "CPF": "TX_CD_CPF",
            "RG": "TX_CD_RG",
            "Sexo": "IN_SEX",
            "Email": "TX_EML",
            "Celular": "TX_CLLR",
            "Endereço": "TX_ENDR",
            "Bairro": "TX_BRRO",
            "Cidade": "TX_CDD",
            "UF": "TX_UF",
            "CEP": "TX_CEP",
            "Classificação no ciclo de vida": "TX_CLSF_CCLO_VDA",
            "Situação no ciclo de vida": "TX_STAC_CCLO_VDA",
            "Inclusão": "TS_DT_INCL",
            "Atualização": "TS_DT_ATT",
            "Origem": "TX_ORGM",
            "NPS": "TX_NPS",
            "Ranking ABC": "TX_RNK_ABC",
            "Valor pago nos últimos 30 dias": "VL_PGO_ULTM_30",
            "Valor pago nos últimos 90 dias": "VL_PGO_ULTM_90",
            "Valor pago nos últimos 180 dias": "VL_PGO_ULTM_180",
            "Valor pago nos últimos 365 dias": "VL_PGO_ULTM_365",
            "Ticket médio": "VL_TCKT_MED",
            "Data da primeira compra": "TS_DT_PRM_CMPR",
            "Última venda": "TS_DT_ULTM_VNDA",
            "Último acesso ao SimplesPet": "TS_DT_ACSO_SMPL_PET",
            "Tags": "TX_TAGS_CLNT",
            "Animais vivos": "TX_ANML_VIVO"
        }

        return df.rename(columns = RENAME_MAP)

    def _treat_frame(self, df, end_date):
        df = df.copy()
        df['TX_ORGM'] = df['TX_ORGM'].fillna('_outros').str.lower()
        df['TS_DT_INCL'] = pd.to_datetime(df['TS_DT_INCL'], dayfirst = True, errors = 'coerce')
        df['TS_DT_INCL'] = df['TS_DT_INCL'].fillna('01/01/1900')
        mask = df['TS_DT_INCL'] <= pd.to_datetime(end_date)
        return df[mask]

    def ingest(self):
        df = super() \
            .pass_options(**self._options) \
            ._read() \
            .pipe(self._rename_columns) \
            .pipe(self._treat_frame, end_date=self._end_date) \

        return df
