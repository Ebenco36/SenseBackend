from abc import ABC, abstractmethod

class Service(ABC):
    @abstractmethod
    def authenticate(self):
        pass

    @abstractmethod
    def fetch(self):
        pass