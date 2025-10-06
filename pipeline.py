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
