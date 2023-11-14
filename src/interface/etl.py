from abc import ABC, abstractmethod

class ETLInterface(ABC):

    @abstractmethod
    def extract(self):
        pass
    
    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def run(self):
        pass