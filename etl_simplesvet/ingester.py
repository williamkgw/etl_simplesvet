from abc import ABC, abstractmethod

class Ingester:
    @abstractmethod
    def ingest(self):
        pass
