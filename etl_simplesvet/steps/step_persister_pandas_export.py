from etl_simplesvet.persisters.persister_pandas_xlsx import PersisterPandasXLSX

from etl_simplesvet.step import Step

class StepPersisterPandasExport(Step):
    def run(self, **kwargs):
        EXPORT_FILE = "datasets/output/out_import_1.xlsx"
        df_export = kwargs["df_export"]

        persister = PersisterPandasXLSX(EXPORT_FILE)

        persister.persist(df_export)

        return {
            **kwargs
        }
