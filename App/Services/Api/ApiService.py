from abc import ABC, abstractmethod

class ApiService(ABC):
    @abstractmethod
    def get_records(self):
        pass