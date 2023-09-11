import requests
from App.Services.Api.ApiService import ApiService

class XmlApiService(ApiService):
    def __init__(self, api_url, headers=None):
        self.api_url = api_url
        self.headers = headers

    def get_records(self):
        response = requests.get(self.api_url, headers=self.headers)
        if response.status_code == 200:
            # Parse XML response and return records
            # Replace this with your XML parsing logic
            return []
        else:
            return []
        
    
    def post_data(self, data):
        response = requests.post(self.api_url, data=data)
        if response.status_code == 200:
            return response.text
        else:
            return ""