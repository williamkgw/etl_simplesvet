from etl_simplesvet.pipeline import Pipeline
from etl_simplesvet.steps.step_ingest_pandas_data_analysis import StepIngestPandasDataAnalysis
from etl_simplesvet.steps.step_transform_pandas_data_analysis_sales import StepTransformPandasDataAnalysisSales
from etl_simplesvet.steps.step_transform_pandas_data_analysis_clients import StepTransformPandasDataAnalysisClients
from etl_simplesvet.steps.step_persister_pandas_data_analysis_sales import StepPersisterPandasDataAnalysisSales
from etl_simplesvet.steps.step_persister_pandas_data_analysis_clients import StepPersisterPandasDataAnalysisClients

from etl_simplesvet.steps.step_other import StepOther

def main():
    ingestion = StepIngestPandasDataAnalysis()
    transform_sales = StepTransformPandasDataAnalysisSales()
    transform_clients = StepTransformPandasDataAnalysisClients()
    persister_sales = StepPersisterPandasDataAnalysisSales()
    persister_clients = StepPersisterPandasDataAnalysisClients()

    other = StepOther()

    pipeline = Pipeline()
    pipeline \
    .add_step(ingestion) \
    .add_step(transform_sales) \
    .add_step(transform_clients) \
    .add_step(persister_sales) \
    .add_step(persister_clients) \
    .run()

if __name__ == "__main__":
    main()
