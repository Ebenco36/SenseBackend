import pandas as pd
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
        return self.authenticate(headers).executeRetrieveData()

    """
        Process flow of retrieving records from EMBASE
        Stages are listed here.
    """
    
    # @retry(exceptions=requests.exceptions.ProxyError, tries=3, delay=2, backoff=2)                
    def retrieveRecord(self, page = 0):
        url = 'https://www.cochranelibrary.com/en/advanced-search/search-manager?p_p_id=scolarissearchresultsportlet_WAR_scolarissearchresults&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=get-query-result-counts&p_p_cacheability=cacheLevelPage&p_p_col_id=column-1&p_p_col_count=2'
        searchManager = {
            "name": "",
            "description": "View all comments View less comments View all comments View less comments View all comments View less comments View all comments View less comments ",
            "rowIndex": 17,
            "rows": [
                {"type": "blank", "row": 1, "text": "'immunization' AND humans", "limits": {}},
                {"type": "manager", "row": 2, "text": "'vaccine' AND humans", "limits": {}},
                {"type": "manager", "row": 3, "text": "(immunisation:ti,ab OR immunization:ti,ab OR immunise:ti,ab OR immunize:ti,ab OR immunising:ti,ab OR immunizing:ti,ab OR immunised:ti,ab OR immunized:ti,ab OR immunises:ti,ab OR immunizes:ti,ab OR (vaccine:ti,ab AND immunity:ti,ab)) AND humans", "limits": {}},
                {"type": "manager", "row": 5, "text": "#1 OR #2 OR #3", "limits": {}},
                {"type": "manager", "row": 6, "text": "'review'", "limits": {}},
                {"type": "manager", "row": 7, "text": "(literature NEAR/3 review*):ti,ab", "limits": {}},
                {"type": "manager", "row": 8, "text": "'meta analysis'", "limits": {}},
                {"type": "manager", "row": 9, "text": "'Systematic Review'", "limits": {}},
                {"type": "manager", "row": 10, "text": "#6 OR #7 OR #8 OR #9", "limits": {}},
                {"type": "manager", "row": 11, "text": "medline:ti,ab OR medlars:ti,ab OR embase:ti,ab OR pubmed:ti,ab OR cinahl:ti,ab OR amed:ti,ab OR psychlit:ti,ab OR psyclit:ti,ab OR psychinfo:ti,ab OR psycinfo:ti,ab OR scisearch:ti,ab OR cochrane:ti,ab", "limits": {}},
                {"type": "manager", "row": 12, "text": "'retraction notice'", "limits": {}},
                {"type": "manager", "row": 13, "text": "#11 OR #12", "limits": {}},
                {"type": "manager", "row": 14, "text": "#10 AND #13", "limits": {}},
                {"type": "manager", "row": 15, "text": "(systematic* NEAR/2 (review* OR overview)):ti,ab", "limits": {}},
                {"type": "manager", "row": 16, "text": "(meta*anal*:ti,ab OR meta:ti,ab) AND anal*:ti,ab OR 'meta anal*':ti,ab OR metaanal*:ti,ab OR metaanal*:ti,ab", "limits": {}},
                {"type": "manager", "row": 17, "text": "#14 OR #15 OR #16", "limits": {}},
                {"type": "manager", "row": 18, "text": "#5 AND #17", "limits": {}}
            ]
        }

        record_details_data = ApiRequest('json', url, headers=self.auth_headers)
        data = record_details_data.send_data(searchManager)
        print(data)
        if (data.get('status') and data.get('status') is True):
            data = data.get('data')
        else:
            print(data)
            data = None
        return data
    
    """
        We are following the structure as described on EMBASE Server.
    """
    def executeRetrieveData(self):
        create_directory_if_not_exists("Cochrane/")
        page = 1
        all_records = []

        # Fetch records for the current page
        records = self.retrieveRecord(page)

        print(records)