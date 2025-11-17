from pandas import ExcelWriter

from etl_simplesvet.persisters.persister_pandas_xlsx import PersisterPandasXLSX

from etl_simplesvet.step import Step


class StepPersisterPandasDataAnalysisClients(Step):
    def run(self, **kwargs):
        clients_df = kwargs["clients_df"]
        agg_clientes = kwargs["agg_clientes"]
        agg_clientes_total = kwargs["agg_clientes_total"]
        agg_v_clientes = kwargs["agg_v_clientes"]

        agg_f = "datasets/output/test_agg_clientes_1.xlsx"
        clients_f = "datasets/output/clientes_csv_1.xlsx"

        persister = PersisterPandasXLSX(clients_f)
        persister.persist(clients_df)
        with ExcelWriter(agg_f) as writer:
            persister.pass_options(excel_writer = writer)

            persister.pass_options(sheet_name = "grupo_clientes").persist(agg_clientes)
            persister.pass_options(sheet_name = "grupo_total").persist(agg_clientes_total)
            persister.pass_options(sheet_name = "ativos_clientes").persist(agg_v_clientes)

        return {
            **kwargs
        }

