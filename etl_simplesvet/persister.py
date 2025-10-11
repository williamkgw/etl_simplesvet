from abc import ABC, abstractmethod

class Persister(ABC):

    @abstractmethod
    def persist(self):
        pass

