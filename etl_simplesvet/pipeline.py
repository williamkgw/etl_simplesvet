class Pipeline:
    def __init__(self):
        self.__steps = list()

    def __iter__(self):
        return self

    def __next__(self):
        if not self.__steps:
            raise StopIteration

        current_step = self.__steps.pop(0)
        return current_step

    def add_step(self, step):
        self.__steps.append(step)
        return self

    def run(self):
        pipeline_ctx = dict()
        step_ctx = dict()

        for step in self:
            step_ctx = step.run(**step_ctx)
            pipeline_ctx.update({step.__class__.__name__: step_ctx})

        return pipeline_ctx

