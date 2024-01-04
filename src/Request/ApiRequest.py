from src.Services.Api.ApiServiceFactory import ApiServiceFactory

class ApiRequest:
    def __init__(self, service_type, api_url, headers=None):
        self.service_type = service_type
        self.api_url = api_url
        self.headers = headers

    def fetch_records(self):
        api_service = ApiServiceFactory.create_service(self.service_type, self.api_url, headers=self.headers)
        return api_service.get_records()
    

    def send_data(self, data):
        api_service = ApiServiceFactory.create_service(self.service_type, self.api_url, headers=self.headers)
        return api_service.post_data(data)