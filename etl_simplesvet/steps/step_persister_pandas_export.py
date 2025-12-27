from etl_simplesvet.persisters.persister_pandas_xlsx import PersisterPandasXLSX

from etl_simplesvet.step import Step


class StepPersisterPandasExport(Step):
    def run(self, **kwargs):
        EXPORT_FILE = "datasets/output/out_import.xlsx"

        df_export = kwargs["df_export"]

        RENAME_MAP = {
            "CD_ID_ITEM": "ID do Item",
            "VL_MES": "Mês",
            "VL_ANO": "Ano",
            "VL_MED": "Medição",
            "TX_FX_VERD_INF_PREV": "Fx Verde Inf/Previsto",
            "TX_FX_VERD_SUP": "Fx Verde Sup",
            "TX_FX_VERM_INF": "Fx Vermelha Inf",
            "TX_FX_VERM_SUP": "Fx Vermelha Sup",
            "TX_FX_CLNT_INF": "Fx Cliente Inf",
            "TX_FX_CLNT_SUP": "Fx Cliente Sup",
            "TX_ITEM": "Item",
            "TX_IND": "Indicador",
            "TX_USUA": "Usuário",
            "TX_TIPO": "Tipo",
            "TX_AUX": "Auxiliar",
            "TX_TOT": "Totalizado",
            "TX_IN_MED": "Medido",
            "TX_CLDR": "Calendário"
        }

        df_export = df_export \
                        .reset_index() \
                        .rename(columns = RENAME_MAP) \
                        .set_index(RENAME_MAP["CD_ID_ITEM"])

        persister = PersisterPandasXLSX(EXPORT_FILE)

        persister.persist(df_export)

        return {
            **kwargs
        }

