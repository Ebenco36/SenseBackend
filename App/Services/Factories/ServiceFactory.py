from App.Services import Service
from App.Services.Factories.EMBASE import EMBASE
from App.Services.Factories.LoveV import LoveV

class ServiceFactory:
    SERVICES = {
        'embase': EMBASE,
        'L.ove': LoveV,
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
