from src.Services import Service
from src.Services.Factories.LoveV import LoveV
from src.Services.Factories.MedlineClass import MedlineClass
from src.Services.Factories.Cochrane import Cochrane
from src.Services.Factories.OVIDNew import OvidJournalDataFetcher 
# OVID new update but still requires manual work a little

class ServiceFactory:
    SERVICES = {
        'L.ove': LoveV,
        'cochrane': Cochrane,
        'medline_class': MedlineClass,
        'ovid_new': OvidJournalDataFetcher
        # Add more services here
    }

    @classmethod
    def create_service(cls, service_type):
        service_class = cls.SERVICES.get(service_type)
        if service_class:
            return service_class()
        else:
            raise ValueError("Invalid service type")
