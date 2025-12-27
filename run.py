from etl_simplesvet.pipeline import Pipeline
from etl_simplesvet.steps.step_ingest_pandas_data_analysis import StepIngestPandasDataAnalysis
from etl_simplesvet.steps.step_transform_pandas_data_analysis_sales import StepTransformPandasDataAnalysisSales
from etl_simplesvet.steps.step_transform_pandas_data_analysis_clients import StepTransformPandasDataAnalysisClients
from etl_simplesvet.steps.step_ingest_pandas_export import StepIngestPandasExport
from etl_simplesvet.steps.step_transform_pandas_export import StepTransformPandasExport
from etl_simplesvet.steps.step_persister_pandas_export import StepPersisterPandasExport

def main():
    ingestion_data_analysis = StepIngestPandasDataAnalysis()
    transform_sales = StepTransformPandasDataAnalysisSales()
    transform_clients = StepTransformPandasDataAnalysisClients()
    ingestion_export = StepIngestPandasExport()
    transform_export = StepTransformPandasExport()
    persister_export = StepPersisterPandasExport()

    pipeline = Pipeline()
    pipeline \
    .add_step(ingestion_data_analysis) \
    .add_step(transform_sales) \
    .add_step(transform_clients) \
    .add_step(ingestion_export) \
    .add_step(transform_export) \
    .add_step(persister_export) \
    .run()

if __name__ == "__main__":
    main()
