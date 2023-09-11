import requests
import json
from App.Services.Api.ApiService import ApiService

class JsonApiService(ApiService):
    def __init__(self, api_url, headers=None):
        self.api_url = api_url
        self.headers = headers

    def get_records(self):
        response = requests.get(self.api_url, headers=self.headers)
        if response.status_code == 200:
            resp = {
                "data": response.json(),
                "status": True
            }
            return resp
        else:
            failed = {
                "data": response.text,
                "status": False
            }

            return failed
        
    def post_data(self, data):
        response = requests.post(self.api_url, json=data)
        if response.status_code == 200:
            resp = {
                "data": response.json(),
                "status": True
            }
            return resp
        else:
            failed = {
                "data": response.text,
                "status": False
            }
            return failed