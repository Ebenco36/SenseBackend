from src.Services.Api.Services.JsonApiService import JsonApiService
from src.Services.Api.Services.XmlApiService import XmlApiService

class ApiServiceFactory:
    SERVICES = {
        'json': JsonApiService,
        'xml': XmlApiService,
    }

    @classmethod
    def create_service(cls, service_type, api_url, headers = None):
        service_class = cls.SERVICES.get(service_type)
        if service_class:
            return service_class(api_url, headers=headers)
        else:
            raise ValueError("Invalid service type")