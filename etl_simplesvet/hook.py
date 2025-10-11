from abc import ABC, abstractmethod

class Hook(ABC):

    @abstractmethod
    def connect(self):
        pass
