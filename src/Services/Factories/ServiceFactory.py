from src.Services import Service
from src.Services.Factories.EMBASE import EMBASE
from src.Services.Factories.LoveV import LoveV
from src.Services.Factories.Medline import Medline
from src.Services.Factories.Cochrane import Cochrane

class ServiceFactory:
    SERVICES = {
        'embase': EMBASE,
        'L.ove': LoveV,
        'medline': Medline,
        'cochrane': Cochrane,
        # Add more services here
    }

    @classmethod
    def create_service(cls, service_type):
        service_class = cls.SERVICES.get(service_type)
        print(service_class)
        if service_class:
            return service_class()
        else:
            raise ValueError("Invalid service type")
