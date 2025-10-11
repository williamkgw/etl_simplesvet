from abc import ABC, abstractmethod

class Transformer(ABC):

    @abstractmethod
    def transform(self):
        pass
