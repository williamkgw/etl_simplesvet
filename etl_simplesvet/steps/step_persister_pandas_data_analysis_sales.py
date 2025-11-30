from pandas import ExcelWriter

from etl_simplesvet.persisters.persister_pandas_xlsx import PersisterPandasXLSX

from etl_simplesvet.step import Step

class StepPersisterPandasDataAnalysisSales(Step):
    def run(self, **kwargs):
        agg_f = "datasets/output/test_agg_1.xlsx"
        vendas_missing_f = "datasets/output/missing_vendas_csv_1.xlsx"
        vendas_csv_f = "datasets/output/vendas_csv_1.xlsx"
        vendas_missing_df = kwargs["vendas_missing_df"]
        sales_df = kwargs["sales_df"]
        agg_grupo_df = kwargs["agg_grupo_df"]
        agg_pilar_df = kwargs["agg_pilar_df"]
        agg_categoria_df = kwargs["agg_categoria_df"]
        agg_tempo_df = kwargs["agg_tempo_df"]
        agg_exception_df = kwargs["agg_exception_df"]
        inadimplencia_df = kwargs["inadimplencia"]
        unique_mapping_df = kwargs["unique_mapping_df"]

        persister = PersisterPandasXLSX(agg_f)

        persister.pass_options(excel_writer = vendas_missing_f).persist(vendas_missing_df)
        persister.pass_options(excel_writer = vendas_csv_f).persist(sales_df)
        with ExcelWriter(agg_f) as writer:
            persister.pass_options(excel_writer = writer)

            persister.pass_options(sheet_name = "grupo").persist(agg_grupo_df)
            persister.pass_options(sheet_name = "pilar").persist(agg_pilar_df)
            persister.pass_options(sheet_name = "categoria").persist(agg_categoria_df)
            persister.pass_options(sheet_name = "total").persist(agg_tempo_df)
            persister.pass_options(sheet_name = "exception").persist(agg_exception_df)
            persister.pass_options(sheet_name = "inadimplencia").persist(inadimplencia_df)
            persister.pass_options(sheet_name = "unique_mapping").persist(unique_mapping_df)

        return {
            **kwargs
        }
