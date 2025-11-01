from etl_simplesvet.pipeline import Pipeline
from etl_simplesvet.steps.step_other import StepOther
from etl_simplesvet.steps.step_ingest_pandas_data_analysis import StepPandasDataAnalysis

def main():
    pipeline = Pipeline()
    step = StepPandasDataAnalysis()
    other_step = StepOther()

    pipeline.add_step(step).add_step(other_step)
    pipeline.run()

if __name__ == "__main__":
    main()
