from src.Services import Service
from src.Services.Factories.EMBASE import EMBASE
from src.Services.Factories.LoveV import LoveV
from src.Services.Factories.Medline import Medline
from src.Services.Factories.Cochrane import Cochrane
from src.Services.Factories.OVID import OVID

class ServiceFactory:
    SERVICES = {
        'embase': EMBASE,
        'L.ove': LoveV,
        'medline': Medline,
        'cochrane': Cochrane,
        'ovid': OVID,
        # Add more services here
    }

    @classmethod
    def create_service(cls, service_type):
        service_class = cls.SERVICES.get(service_type)
        if service_class:
            return service_class()
        else:
            raise ValueError("Invalid service type")
