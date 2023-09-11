from App.Services.Service import Service
from App.Request.ApiRequest import ApiRequest
from App.Utils.Helpers import save_json_to_csv

class LoveV(Service):

    def authenticate(self, header):
        self.auth_header = header

    def fetch(self):
        print("Welcome")

    """
        Process flow of retrieving records from EMBASE
        Stages are listed here.
    """
    