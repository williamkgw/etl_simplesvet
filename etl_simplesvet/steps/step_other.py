from etl_simplesvet.step import Step

class StepOther(Step):
    def run(self, **kwargs):
        print(kwargs)
        print("Other Step to run")

