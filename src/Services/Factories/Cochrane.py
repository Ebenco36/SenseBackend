import pandas as pd
from src.Commands.services import fetch_first_scholar_result
from src.Services.Service import Service
from src.Request.ApiRequest import ApiRequest
from src.Utils.Helpers import save_json_to_csv
from src.Utils.Helpers import create_directory_if_not_exists, save_json_to_csv, append_json_response_to_file, \
    json_to_dataframe_and_save, get_remainder_and_quotient, convert_json_to_List_of_dict


class Cochrane(Service):
    # token from cochrane fca8eec8-dc0c-4d58-99f0-11483bbc5ad0
    # https://onlinelibrary.wiley.com/library-info/resources/text-and-datamining
    def __init__(self, pageSize = 200):
        self.pageSize = pageSize


    def authenticate(self, headers):
        self.auth_headers = headers
        return self

    def fetch(self, headers):
        return self.authenticate(headers).retrieveRecord()

    """
        Process flow of retrieving records from EMBASE
        Stages are listed here.
    """
    
    # @retry(exceptions=requests.exceptions.ProxyError, tries=3, delay=2, backoff=2)                
    def retrieveRecord(self, page = 0):
        # We manually spool data. Implementation below is to fetch doi link for paper with doi from google scholar
        # Apply the function to each row in the DataFrame
        df = pd.read_csv("./Cochrane/citation-export.csv")
        df['DOI'] = df['Title'].apply(fetch_first_scholar_result)
        df.to_csv("./Cochrane/citation-export.csv", index=False)