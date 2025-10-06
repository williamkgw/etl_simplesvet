from pipeline import Pipeline
from step import StepPandas, StepOther

def main():
    pipeline = Pipeline()
    step = StepPandas()
    other_step = StepOther()

    pipeline.add_step(other_step).add_step(step).add_step(other_step)

    for step in pipeline:
        step.run()

if __name__ == "__main__":
    main()
