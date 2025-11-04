from etl_simplesvet.pipeline import Pipeline
from etl_simplesvet.steps.step_ingest_pandas_data_analysis import StepIngestPandasDataAnalysis
from etl_simplesvet.steps.step_transform_pandas_data_analysis_clients import StepTransformPandasDataAnalysisClients
from etl_simplesvet.steps.step_transform_pandas_data_analysis_sales import StepTransformPandasDataAnalysisSales

from etl_simplesvet.steps.step_other import StepOther

def main():
    ingestion = StepIngestPandasDataAnalysis()
    transform_sales = StepTransformPandasDataAnalysisSales()
    transform_clients = StepTransformPandasDataAnalysisClients()
    other = StepOther()

    pipeline = Pipeline()
    pipeline \
    .add_step(ingestion) \
    .add_step(transform_sales) \
    .add_step(transform_clients) \
    .add_step(other) \
    .run()

if __name__ == "__main__":
    main()
